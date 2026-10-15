"""
Serveur média — Projet 18/19 (diffusion vidéo en continu, adapté au web)
--------------------------------------------------------------------------
Modèle DIFFUSEUR / SPECTATEURS :
  - Un DIFFUSEUR publie un flux (caméra ou fichier vidéo) dans un salon.
  - Un ou plusieurs SPECTATEURS rejoignent ce salon en lecture seule et
    reçoivent ce flux, relayé par le serveur (ils ne publient rien).

Ce serveur combine deux rôles :

1. SIGNALISATION : reçoit les offres/réponses SDP de chaque client via
   un WebSocket, pour établir une connexion WebRTC (RTCPeerConnection)
   entre le navigateur et CE serveur (pas entre navigateurs directement).

2. RELAIS (SFU - Selective Forwarding Unit) : le flux publié par le
   diffuseur est retransmis à chaque spectateur du même salon, sans le
   décoder/réencoder (relais léger).

Architecture :

                              ┌─────────────────┐
    Diffuseur ──WebRTC (envoi)──▶  Serveur média  ──WebRTC (réception)──▶ Spectateur A
                              │   (relais aiortc) │──WebRTC (réception)──▶ Spectateur B
                              └─────────────────┘──WebRTC (réception)──▶ Spectateur C

Un seul diffuseur est autorisé par salon à la fois (le serveur refuse
un deuxième diffuseur tant que le premier n'est pas parti).

CHOIX D'IMPLÉMENTATION IMPORTANT — pas de renégociation SDP :
Un spectateur réserve, dès son offre initiale, deux emplacements
("transceivers" recvonly, un pour la vidéo et un pour l'audio — voir
useWatchRoom.ts). Le serveur n'ajoute donc JAMAIS de piste après coup
via une nouvelle offre (renégociation) : il réutilise ces emplacements
déjà négociés avec `RTCRtpSender.replaceTrack()`, qui ne modifie pas le
SDP et ne nécessite aucun aller-retour de signalisation supplémentaire.

Ce choix contourne un bug connu et non résolu d'aiortc où une
renégociation initiée par le serveur peut réattribuer différemment les
identifiants d'extensions RTP (extmap) par rapport à la négociation
initiale, ce que les navigateurs refusent avec une erreur du type
"Remote description attempted to remap RTP extension id..."
(voir https://github.com/aiortc/aiortc/issues/686). Comme le mainteneur
du projet le note lui-même, l'erreur n'apparaît pas quand c'est le
CLIENT qui négocie tout depuis le début — c'est exactement ce que fait
cette implémentation.

Protocole de signalisation (messages JSON échangés sur le WebSocket) :
    Connexion : ws://<host>/ws/{room_id}/{user_id}?role=broadcaster|viewer

    Client -> Serveur : {"type": "offer", "sdp": "..."}
    Serveur -> Client  : {"type": "answer", "sdp": "..."}
    Serveur -> Client  : {"type": "error", "message": "..."}  (ex : salon déjà
                          diffusé par quelqu'un d'autre)
    Serveur -> Client  : {"type": "broadcaster-left"}          (le diffuseur
                          a quitté le salon)

Démarrage :
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Dépendances système requises par aiortc :
    ffmpeg / libavcodec / libavformat (voir README.md)
"""

import asyncio
import json
import logging
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaRelay

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("media-server")

app = FastAPI(title="Serveur média - Diffusion vidéo (aiortc)")

# En développement, on autorise le frontend Next.js (localhost:3000) à se
# connecter. En production, restreindre à l'origine réelle du site.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# MediaRelay permet de dupliquer un même flux source vers plusieurs
# abonnés (les spectateurs) sans consommer la track originale plusieurs fois.
relay = MediaRelay()


class Peer:
    """Représente la connexion WebRTC d'un participant avec le serveur."""

    def __init__(self, user_id: str, role: str, websocket: WebSocket):
        self.user_id = user_id
        self.role = role  # "broadcaster" ou "viewer"
        self.websocket = websocket
        self.pc = RTCPeerConnection()
        # Tracks publiées par ce participant (flux original, non relayé).
        # Reste vide pour un spectateur, qui ne publie jamais rien.
        self.published_tracks = []
        # Pour un SPECTATEUR uniquement : transceivers "recvonly" déjà
        # négociés lors de l'offre initiale, indexés par type de média
        # ("video"/"audio"). Permet d'y attacher un flux plus tard via
        # replaceTrack() sans jamais renégocier le SDP.
        self.transceivers_by_kind = {}
        # Protège l'unique négociation SDP de ce pc (offre/réponse initiale).
        self.negotiation_lock = asyncio.Lock()


class Room:
    """Un salon de diffusion : un diffuseur, zéro ou plusieurs spectateurs."""

    def __init__(self, room_id: str):
        self.room_id = room_id
        self.peers: Dict[str, Peer] = {}
        self.broadcaster_id: Optional[str] = None

    def other_peers(self, user_id: str):
        return [p for uid, p in self.peers.items() if uid != user_id]

    @property
    def broadcaster(self) -> Optional[Peer]:
        if self.broadcaster_id is None:
            return None
        return self.peers.get(self.broadcaster_id)

    @property
    def viewer_count(self) -> int:
        return len(self.peers) - (1 if self.broadcaster_id else 0)


rooms: Dict[str, Room] = {}


def get_or_create_room(room_id: str) -> Room:
    if room_id not in rooms:
        rooms[room_id] = Room(room_id)
        logger.info("Salon créé : %s", room_id)
    return rooms[room_id]


async def send_json(websocket: WebSocket, message: dict):
    await websocket.send_text(json.dumps(message))


def attach_track_to_viewer(viewer: Peer, track) -> None:
    """
    Attache `track` (relayée) au transceiver déjà négocié du spectateur,
    pour son type de média (vidéo ou audio). N'effectue AUCUNE
    renégociation SDP : replaceTrack() change juste le flux envoyé sur
    un emplacement déjà établi.
    """
    transceiver = viewer.transceivers_by_kind.get(track.kind)
    if transceiver is None:
        logger.warning(
            "Aucun transceiver %s réservé pour %s, impossible de relayer.",
            track.kind,
            viewer.user_id,
        )
        return
    transceiver.sender.replaceTrack(relay.subscribe(track))


async def broadcast_new_track_to_room(room: Room, publisher_id: str, track):
    """Relaie UNE piste nouvellement publiée à tous les spectateurs du salon."""
    for viewer in room.other_peers(publisher_id):
        attach_track_to_viewer(viewer, track)


async def cleanup_peer(room: Room, user_id: str):
    peer = room.peers.pop(user_id, None)
    if peer is None:
        # Déjà nettoyé par un appel précédent (ex : à la fois par le
        # handler `connectionstatechange` ET par le bloc `finally` du
        # websocket). Sortir tout de suite évite de traiter ce salon une
        # deuxième fois plus bas.
        return

    was_broadcaster = room.broadcaster_id == user_id

    await peer.pc.close()
    logger.info(
        "Participant déconnecté : %s (%s, salon %s)",
        user_id,
        peer.role,
        room.room_id,
    )

    if was_broadcaster:
        room.broadcaster_id = None
        # Les spectateurs restants gardent leurs transceivers négociés :
        # on retire juste le flux (silence/écran figé) en attendant un
        # éventuel nouveau diffuseur.
        for remaining in room.peers.values():
            for transceiver in remaining.transceivers_by_kind.values():
                transceiver.sender.replaceTrack(None)

    if not room.peers:
        # N'enlève ce salon du dictionnaire global QUE s'il s'agit bien de
        # CETTE instance (et pas d'un salon plus récent qui porterait par
        # coïncidence le même identifiant, créé entre-temps après qu'un
        # ancien salon vide ait déjà été retiré).
        if rooms.get(room.room_id) is room:
            rooms.pop(room.room_id, None)
            logger.info("Salon fermé (vide) : %s", room.room_id)
    else:
        for remaining in room.peers.values():
            try:
                if was_broadcaster:
                    # Le diffuseur est parti : les spectateurs restants
                    # doivent le savoir (message "diffusion terminée").
                    await send_json(remaining.websocket, {"type": "broadcaster-left"})
                else:
                    await send_json(
                        remaining.websocket,
                        {"type": "peer-left", "user_id": user_id},
                    )
            except RuntimeError:
                # Le websocket du destinataire est déjà fermé (déconnexions
                # simultanées) : rien à faire, il sera nettoyé à son tour.
                pass


@app.get("/")
async def health_check():
    """Petite route de vérification que le serveur tourne."""
    return {
        "status": "ok",
        "salons_actifs": len(rooms),
        "participants_total": sum(len(r.peers) for r in rooms.values()),
    }


@app.get("/rooms/{room_id}")
async def room_status(room_id: str):
    """Permet au frontend de savoir si un salon est déjà en cours de diffusion."""
    room = rooms.get(room_id)
    if room is None:
        return {"exists": False, "has_broadcaster": False, "viewer_count": 0}
    return {
        "exists": True,
        "has_broadcaster": room.broadcaster_id is not None,
        "viewer_count": room.viewer_count,
    }


@app.websocket("/ws/{room_id}/{user_id}")
async def signaling_endpoint(websocket: WebSocket, room_id: str, user_id: str):
    """
    Point d'entrée WebSocket : un participant rejoint `room_id` en tant
    que `user_id`, avec un rôle passé en query string : ?role=broadcaster
    ou ?role=viewer (viewer par défaut si absent).
    """
    role = websocket.query_params.get("role", "viewer")
    if role not in ("broadcaster", "viewer"):
        role = "viewer"

    await websocket.accept()
    room = get_or_create_room(room_id)

    # Un seul diffuseur à la fois par salon.
    if role == "broadcaster" and room.broadcaster_id is not None:
        await send_json(
            websocket,
            {
                "type": "error",
                "message": "Ce salon a déjà un diffuseur actif. Réessayez plus tard.",
            },
        )
        await websocket.close()
        return

    peer = Peer(user_id, role, websocket)
    room.peers[user_id] = peer
    if role == "broadcaster":
        room.broadcaster_id = user_id
    logger.info("Participant connecté : %s (%s, salon %s)", user_id, role, room_id)

    @peer.pc.on("track")
    def on_track(track):
        if peer.role != "broadcaster":
            # Un spectateur ne devrait jamais publier de flux ; on ignore
            # par sécurité (le frontend ne devrait pas non plus en envoyer).
            logger.warning("Track inattendue reçue d'un spectateur : %s", user_id)
            return
        logger.info("Track reçue du diffuseur %s : %s", user_id, track.kind)
        peer.published_tracks.append(track)
        # Relaie uniquement CETTE nouvelle track à tous les spectateurs
        # déjà présents dans le salon (pas les tracks déjà relayées avant).
        asyncio.ensure_future(broadcast_new_track_to_room(room, user_id, track))

        @track.on("ended")
        async def on_ended():
            if track in peer.published_tracks:
                peer.published_tracks.remove(track)

    @peer.pc.on("connectionstatechange")
    async def on_connection_state_change():
        logger.info(
            "État de connexion de %s : %s", user_id, peer.pc.connectionState
        )
        if peer.pc.connectionState in ("failed", "closed", "disconnected"):
            await cleanup_peer(room, user_id)

    try:
        while True:
            raw_message = await websocket.receive_text()
            data = json.loads(raw_message)
            message_type = data.get("type")

            if message_type == "offer":
                # Offre initiale et UNIQUE du client (aucune renégociation
                # n'aura lieu ensuite, voir la note en tête de fichier).
                async with peer.negotiation_lock:
                    await peer.pc.setRemoteDescription(
                        RTCSessionDescription(sdp=data["sdp"], type="offer")
                    )

                    if role == "viewer":
                        # Mémorise les transceivers recvonly réservés par
                        # le spectateur, pour pouvoir y attacher un flux
                        # plus tard sans jamais renégocier.
                        #
                        # IMPORTANT : on force explicitement la direction à
                        # "sendonly" dès maintenant, MÊME s'il n'y a encore
                        # aucun flux à envoyer (aucun diffuseur actif). Sans
                        # ça, aiortc négocierait ces emplacements comme
                        # "inactive" (rien à envoyer au moment de la
                        # réponse), et un replaceTrack() ultérieur ne
                        # suffirait pas à faire circuler le média : la
                        # direction négociée dans le SDP prime sur le seul
                        # fait qu'une track soit attachée au sender.
                        for transceiver in peer.pc.getTransceivers():
                            transceiver.direction = "sendonly"
                            peer.transceivers_by_kind[transceiver.kind] = transceiver

                        # Si un diffuseur est déjà actif, attache
                        # immédiatement son flux à ces emplacements.
                        for other in room.other_peers(user_id):
                            for track in other.published_tracks:
                                attach_track_to_viewer(peer, track)

                    answer = await peer.pc.createAnswer()
                    await peer.pc.setLocalDescription(answer)
                    await send_json(
                        websocket,
                        {"type": "answer", "sdp": peer.pc.localDescription.sdp},
                    )

            else:
                logger.warning("Message de signalisation inconnu : %s", data)

    except WebSocketDisconnect:
        pass
    finally:
        await cleanup_peer(room, user_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
