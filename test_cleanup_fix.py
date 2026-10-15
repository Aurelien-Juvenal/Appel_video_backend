"""
Test unitaire ciblé : reproduit précisément le bug corrigé, sans dépendre
du timing réseau réel (WebRTC/ICE) qui est difficile à contrôler dans un
test automatisé.

Scénario reproduit :
  1. Un premier salon "A" est créé pour un diffuseur (room_id "X").
  2. Ce diffuseur est nettoyé deux fois de suite (comme observé en
     conditions réelles : une fois via `connectionstatechange`, une fois
     via le bloc `finally` du websocket) -> le salon A est bien supprimé
     une seule fois.
  3. Un NOUVEAU salon "B" est créé entre-temps, avec le MÊME room_id "X"
     (cas réel : un deuxième diffuseur se connecte au même salon).
  4. Le deuxième appel de nettoyage tardif pour le diffuseur du salon A
     ne doit PAS supprimer le salon B (bug corrigé).
"""

import asyncio
import main as media_server


async def run():
    media_server.rooms.clear()

    # 1. Salon A avec un diffuseur
    room_a = media_server.get_or_create_room("X")
    peer_a = media_server.Peer("broadcaster-A", "broadcaster", websocket=None)
    room_a.peers["broadcaster-A"] = peer_a
    room_a.broadcaster_id = "broadcaster-A"

    # Empêche l'appel réseau réel de peer.pc.close()
    async def fake_close():
        return None

    peer_a.pc.close = fake_close

    # 2. Premier nettoyage (ex: déclenché par connectionstatechange) :
    #    le salon A devient vide et est retiré du dictionnaire global.
    await media_server.cleanup_peer(room_a, "broadcaster-A")
    assert "X" not in media_server.rooms, "Le salon A aurait dû être supprimé"

    # 3. Un NOUVEAU salon B est créé avec le même room_id "X"
    #    (un deuxième diffuseur rejoint le même salon)
    room_b = media_server.get_or_create_room("X")
    peer_b = media_server.Peer("broadcaster-B", "broadcaster", websocket=None)
    room_b.peers["broadcaster-B"] = peer_b
    room_b.broadcaster_id = "broadcaster-B"
    assert media_server.rooms["X"] is room_b

    # 4. Nettoyage TARDIF et redondant pour le diffuseur du salon A
    #    (simule le bloc `finally` qui s'exécute après coup)
    await media_server.cleanup_peer(room_a, "broadcaster-A")

    # ✅ Le salon B doit toujours exister et ne pas avoir été touché
    assert "X" in media_server.rooms, "BUG : le salon B a été supprimé par erreur !"
    assert media_server.rooms["X"] is room_b
    assert "broadcaster-B" in room_b.peers

    print("✅ Succès : le nettoyage tardif du salon A n'a pas affecté le salon B.")


if __name__ == "__main__":
    asyncio.run(run())
