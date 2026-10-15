"""
Test : un spectateur rejoint le salon AVANT que le diffuseur ne commence
à publier. Vérifie qu'il reçoit bien le flux dès que le diffuseur se
connecte, uniquement via replaceTrack() côté serveur (donc SANS aucune
renégociation SDP supplémentaire après la connexion initiale).
"""

import asyncio
import json

from aiortc import RTCPeerConnection, RTCSessionDescription, VideoStreamTrack
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


async def viewer_waits_then_receives(room_id: str, received: asyncio.Event):
    pc = RTCPeerConnection()
    pc.addTransceiver("video", direction="recvonly")

    @pc.on("track")
    def on_track(track):
        print(f"[viewer] Track reçue (sans renégociation) : {track.kind}", flush=True)
        received.set()

    uri = f"ws://localhost:8000/ws/{room_id}/early-viewer?role=viewer"
    async with websockets.connect(uri) as ws:
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        await ws.send(json.dumps({"type": "offer", "sdp": pc.localDescription.sdp}))

        # N'accepte QUE des messages "answer" : si le serveur envoyait encore
        # une "offer" de renégociation, ce test échouerait explicitement ici.
        raw = await ws.recv()
        data = json.loads(raw)
        assert data["type"] == "answer", f"Message inattendu (protocole cassé ?) : {data}"
        await pc.setRemoteDescription(
            RTCSessionDescription(sdp=data["sdp"], type="answer")
        )

        # Attend maintenant la réception de la track, sans plus aucun
        # échange de signalisation nécessaire.
        await received.wait()


async def broadcaster(room_id: str):
    pc = RTCPeerConnection()
    pc.addTrack(FakeVideoTrack())
    uri = f"ws://localhost:8000/ws/{room_id}/late-broadcaster?role=broadcaster"
    async with websockets.connect(uri) as ws:
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        await ws.send(json.dumps({"type": "offer", "sdp": pc.localDescription.sdp}))
        await ws.recv()  # answer
        await asyncio.sleep(10)  # reste connecté le temps du test


async def main():
    room_id = "early-viewer-room"
    received = asyncio.Event()

    task_viewer = asyncio.create_task(viewer_waits_then_receives(room_id, received))
    await asyncio.sleep(0.5)  # le spectateur est bien connecté en premier
    task_broadcaster = asyncio.create_task(broadcaster(room_id))

    try:
        await asyncio.wait_for(task_viewer, timeout=15)
        print("\n✅ Succès : le spectateur connecté en premier a reçu le flux sans renégociation.")
    except asyncio.TimeoutError:
        print("\n❌ Échec : le spectateur n'a rien reçu.")
    finally:
        task_broadcaster.cancel()


if __name__ == "__main__":
    asyncio.run(main())
