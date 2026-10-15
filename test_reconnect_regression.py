"""
Test de régression : simule le scénario exact rencontré en développement
(React Strict Mode) — un diffuseur se connecte puis se déconnecte presque
aussitôt (première tentative annulée), un second diffuseur avec le même
room_id prend le relais, puis un spectateur rejoint. Vérifie que le
spectateur reçoit bien le flux du second diffuseur (et non un salon
"fantôme" déconnecté).
"""

import asyncio
import json

from aiortc import RTCPeerConnection, VideoStreamTrack
import websockets


class FakeVideoTrack(VideoStreamTrack):
    async def recv(self):
        from av import VideoFrame
        import numpy as np

        pts, time_base = await self.next_timestamp()
        arr = np.zeros((240, 320, 3), dtype="uint8")
        arr[:, :] = (30, 200, 180)
        frame = VideoFrame.from_ndarray(arr, format="bgr24")
        frame.pts = pts
        frame.time_base = time_base
        return frame


async def flaky_broadcaster(room_id: str, user_id: str):
    """Se connecte puis se déconnecte immédiatement (simule le double-mount React)."""
    pc = RTCPeerConnection()
    pc.addTrack(FakeVideoTrack())
    uri = f"ws://localhost:8000/ws/{room_id}/{user_id}?role=broadcaster"
    ws = await websockets.connect(uri)
    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)
    await ws.send(json.dumps({"type": "offer", "sdp": pc.localDescription.sdp}))
    await asyncio.sleep(0.05)  # à peine le temps d'ouvrir la connexion
    await ws.close()
    await pc.close()


async def stable_broadcaster(room_id: str, user_id: str, ready: asyncio.Event):
    pc = RTCPeerConnection()
    pc.addTrack(FakeVideoTrack())
    uri = f"ws://localhost:8000/ws/{room_id}/{user_id}?role=broadcaster"
    async with websockets.connect(uri) as ws:
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        await ws.send(json.dumps({"type": "offer", "sdp": pc.localDescription.sdp}))
        ready.set()
        async for raw in ws:
            data = json.loads(raw)
            if data["type"] == "answer":
                await pc.setRemoteDescription({"type": "answer", "sdp": data["sdp"]})
            elif data["type"] == "offer":
                await pc.setRemoteDescription({"type": "offer", "sdp": data["sdp"]})
                answer = await pc.createAnswer()
                await pc.setLocalDescription(answer)
                await ws.send(json.dumps({"type": "answer", "sdp": pc.localDescription.sdp}))


async def viewer(room_id: str, user_id: str, received: asyncio.Event):
    pc = RTCPeerConnection()
    pc.addTransceiver("video", direction="recvonly")

    @pc.on("track")
    def on_track(track):
        print(f"[{user_id}] Track reçue : {track.kind}", flush=True)
        received.set()

    uri = f"ws://localhost:8000/ws/{room_id}/{user_id}?role=viewer"
    async with websockets.connect(uri) as ws:
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        await ws.send(json.dumps({"type": "offer", "sdp": pc.localDescription.sdp}))
        async for raw in ws:
            data = json.loads(raw)
            if data["type"] == "answer":
                await pc.setRemoteDescription({"type": "answer", "sdp": data["sdp"]})
            elif data["type"] == "offer":
                await pc.setRemoteDescription({"type": "offer", "sdp": data["sdp"]})
                answer = await pc.createAnswer()
                await pc.setLocalDescription(answer)
                await ws.send(json.dumps({"type": "answer", "sdp": pc.localDescription.sdp}))
            if received.is_set():
                break
    await pc.close()


async def main():
    room_id = "regression-room"

    # 1. Un premier diffuseur se connecte puis se déconnecte tout de suite
    await flaky_broadcaster(room_id, "flaky1")
    await asyncio.sleep(0.3)

    # 2. Un second diffuseur (même salon) prend le relais et reste connecté
    ready = asyncio.Event()
    task_stable = asyncio.create_task(stable_broadcaster(room_id, "stable1", ready))
    await ready.wait()
    await asyncio.sleep(0.5)

    # 3. Un spectateur rejoint ensuite
    received = asyncio.Event()
    try:
        await asyncio.wait_for(viewer(room_id, "viewer1", received), timeout=15)
        print("\n✅ Succès : le spectateur a reçu le flux malgré la reconnexion rapide.")
    except asyncio.TimeoutError:
        print("\n❌ Échec : le spectateur n'a rien reçu (bug de salon fantôme toujours présent).")
    finally:
        task_stable.cancel()


if __name__ == "__main__":
    asyncio.run(main())
