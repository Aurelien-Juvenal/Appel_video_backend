"""
Test rapide du serveur média : simule un diffuseur et deux spectateurs
qui rejoignent le même salon, et vérifie que chaque spectateur reçoit
bien le flux vidéo du diffuseur (relais fonctionnel).

Utilisation :
    python test_relay.py
    (le serveur doit tourner sur localhost:8000, ou lancez-le d'abord
    avec : uvicorn main:app --port 8000 &)
"""

import asyncio
import json

from aiortc import RTCPeerConnection, RTCSessionDescription, VideoStreamTrack
import websockets


class FakeVideoTrack(VideoStreamTrack):
    """Piste vidéo factice (mire de couleur) pour le test, sans dépendre d'une webcam."""

    async def recv(self):
        from av import VideoFrame
        import numpy as np

        pts, time_base = await self.next_timestamp()
        frame_array = np.zeros((240, 320, 3), dtype=np.uint8)
        frame_array[:, :] = (30, 200, 180)  # couleur unie
        frame = VideoFrame.from_ndarray(frame_array, format="bgr24")
        frame.pts = pts
        frame.time_base = time_base
        return frame


async def run_broadcaster(room_id: str, ready_event: asyncio.Event):
    pc = RTCPeerConnection()
    pc.addTrack(FakeVideoTrack())

    uri = f"ws://localhost:8000/ws/{room_id}/broadcaster1?role=broadcaster"
    async with websockets.connect(uri) as ws:
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        await ws.send(json.dumps({"type": "offer", "sdp": pc.localDescription.sdp}))

        ready_event.set()  # signale que le diffuseur est connecté

        async for raw in ws:
            data = json.loads(raw)
            if data["type"] == "answer":
                await pc.setRemoteDescription(
                    RTCSessionDescription(sdp=data["sdp"], type="answer")
                )
            elif data["type"] == "offer":
                await pc.setRemoteDescription(
                    RTCSessionDescription(sdp=data["sdp"], type="offer")
                )
                answer = await pc.createAnswer()
                await pc.setLocalDescription(answer)
                await ws.send(
                    json.dumps({"type": "answer", "sdp": pc.localDescription.sdp})
                )


async def run_viewer(room_id: str, user_id: str, received_track_event: asyncio.Event):
    pc = RTCPeerConnection()
    pc.addTransceiver("video", direction="recvonly")

    @pc.on("track")
    def on_track(track):
        print(f"[{user_id}] Track reçue du diffuseur : {track.kind}")
        received_track_event.set()

    uri = f"ws://localhost:8000/ws/{room_id}/{user_id}?role=viewer"
    async with websockets.connect(uri) as ws:
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)
        await ws.send(json.dumps({"type": "offer", "sdp": pc.localDescription.sdp}))

        async for raw in ws:
            data = json.loads(raw)
            if data["type"] == "answer":
                await pc.setRemoteDescription(
                    RTCSessionDescription(sdp=data["sdp"], type="answer")
                )
            elif data["type"] == "offer":
                await pc.setRemoteDescription(
                    RTCSessionDescription(sdp=data["sdp"], type="offer")
                )
                answer = await pc.createAnswer()
                await pc.setLocalDescription(answer)
                await ws.send(
                    json.dumps({"type": "answer", "sdp": pc.localDescription.sdp})
                )

            if received_track_event.is_set():
                break

    await pc.close()


async def main():
    room_id = "test-room"
    broadcaster_ready = asyncio.Event()
    viewer_a_event = asyncio.Event()
    viewer_b_event = asyncio.Event()

    task_broadcaster = asyncio.create_task(run_broadcaster(room_id, broadcaster_ready))
    await broadcaster_ready.wait()
    await asyncio.sleep(0.5)  # laisse le diffuseur finaliser sa connexion

    task_viewer_a = asyncio.create_task(run_viewer(room_id, "viewerA", viewer_a_event))
    task_viewer_b = asyncio.create_task(run_viewer(room_id, "viewerB", viewer_b_event))

    try:
        await asyncio.wait_for(
            asyncio.gather(task_viewer_a, task_viewer_b), timeout=15
        )
        print("\n✅ Succès : les deux spectateurs ont bien reçu le flux du diffuseur.")
    except asyncio.TimeoutError:
        print("\n❌ Échec : timeout, le relais n'a pas fonctionné comme attendu.")
    finally:
        task_broadcaster.cancel()


if __name__ == "__main__":
    asyncio.run(main())
