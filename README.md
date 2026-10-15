# Serveur média — Diffusion vidéo (Python + aiortc)

Serveur WebRTC qui reçoit le flux (caméra ou fichier vidéo) de chaque
participant d'un salon et le relaie aux autres (architecture SFU —
Selective Forwarding Unit). Combine signalisation (WebSocket) et relais
média dans un seul service FastAPI, pour rester simple à déployer pour
ce projet.

## Installation

```bash
python -m venv venv
source venv/bin/activate      # Windows : venv\Scripts\activate

pip install -r requirements.txt
```

### Prérequis système (obligatoires pour aiortc)
aiortc s'appuie sur FFmpeg pour l'encodage/décodage :

- **Windows** : téléchargez FFmpeg et ajoutez-le au PATH.
- **Mac** : `brew install ffmpeg`
- **Linux (Ubuntu/Debian)** :
  ```bash
  sudo apt install ffmpeg libavdevice-dev libavfilter-dev libopus-dev libvpx-dev
  ```

## Démarrage

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Vérifiez que le serveur tourne :
```bash
curl http://localhost:8000/
# {"status":"ok","salons_actifs":0,"participants_total":0}
```

## Intégration avec le frontend Next.js

Dans le fichier `.env.local` du frontend, configurez :
```
NEXT_PUBLIC_SIGNALING_URL=ws://localhost:8000
```

⚠️ **Le hook `useCallRoom.ts` du frontend doit être mis à jour** pour :
1. Se connecter sur `/ws/{roomId}/{userId}` (actuellement il ne passe
   pas encore de `userId` — un identifiant unique par participant, ex.
   généré avec `crypto.randomUUID()`).
2. Envoyer une offre SDP réelle (`RTCPeerConnection.createOffer()`)
   après connexion au WebSocket, au lieu de se contenter d'écouter.
3. Traiter les messages `{"type": "offer", ...}` reçus du serveur
   (renégociations) en répondant par une `answer`.
4. Peupler `participants` avec les tracks reçues via l'event `track`
   de la `RTCPeerConnection` locale.

C'est l'étape suivante logique — dites-moi si vous voulez que je la
fasse maintenant.

## Protocole de signalisation

Messages JSON échangés sur `ws://<host>/ws/{room_id}/{user_id}` :

| Émetteur | Message | Quand |
|---|---|---|
| Client | `{"type": "offer", "sdp": "..."}` | À la connexion, pour publier son flux |
| Serveur | `{"type": "answer", "sdp": "..."}` | Réponse à l'offre initiale |
| Serveur | `{"type": "offer", "sdp": "..."}` | Renégociation (nouveau participant à relayer) |
| Client | `{"type": "answer", "sdp": "..."}` | Réponse à une renégociation |
| Serveur | `{"type": "peer-left", "user_id": "..."}` | Un participant a quitté le salon |

## Test rapide (sans navigateur)

`test_relay.py` simule deux clients Python qui rejoignent le même
salon et vérifie que chacun reçoit bien le flux vidéo (factice) de
l'autre :

```bash
# Terminal 1
uvicorn main:app --port 8000

# Terminal 2
pip install websockets numpy
python test_relay.py
```

Résultat attendu :
```
✅ Succès : chaque participant a bien reçu la track de l'autre.
```

## Limites connues (projet étudiant)

- **Pas de trickle ICE** : le serveur attend que la collecte ICE soit
  terminée avant d'envoyer une offre/réponse (approche "vanilla ICE"),
  plus simple à implémenter mais un peu plus lente à la connexion que
  le trickle ICE utilisé en production.
- **Pas de TURN configuré** : fonctionne en réseau local ou entre
  machines avec IP publique directe. Pour des connexions à travers des
  NAT/pare-feu stricts, il faudra ajouter un serveur TURN (ex. coturn)
  et le configurer dans la `RTCConfiguration` du frontend.
- **Stockage en mémoire** : les salons et participants sont perdus si
  le serveur redémarre (pas de persistance).
- **Scalabilité** : convient à quelques salons avec peu de participants
  chacun (usage pédagogique). Pour beaucoup d'utilisateurs simultanés,
  une SFU plus performante (LiveKit, mediasoup) serait recommandée.
