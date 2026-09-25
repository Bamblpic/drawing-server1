import asyncio
import websockets
import json
import time

# ==================== ХРАНИЛИЩА ====================
drawing_history = []           # Готовые штрихи (сохраняются)
MAX_HISTORY = 10000

connected_clients = set()      # Все подключённые клиенты
users = {}                     # {websocket: nickname}

# Буфер «живых» точек для каждого пользователя
# {websocket: [points]}
live_buffers = {}

# ==================== BROADCAST ====================
async def broadcast(message, exclude=None):
    """Отправить сообщение всем клиентам, кроме exclude"""
    if exclude:
        filtered = connected_clients - {exclude}
    else:
        filtered = connected_clients

    if filtered:
        await asyncio.gather(
            *[client.send(message) for client in filtered],
            return_exceptions=True
        )

# ==================== HANDLER ====================
async def handler(websocket, path):
    connected_clients.add(websocket)
    print(f"🔌 Новый клиент (всего: {len(connected_clients)})")

    try:
        # Первое сообщение — никнейм
        welcome_msg = await websocket.recv()
        data = json.loads(welcome_msg)

        if data.get("type") == "join":
            nickname = data.get("nickname", "Аноним")
            users[websocket] = nickname

            # Отправляем историю
            if drawing_history:
                await websocket.send(json.dumps({
                    "type": "history",
                    "strokes": drawing_history
                }))

            # Оповещаем остальных
            await broadcast(json.dumps({
                "type": "user_joined",
                "nickname": nickname,
                "users_count": len(connected_clients)
            }), exclude=websocket)

            print(f"👤 {nickname} присоединился")

        # Основной цикл
        async for message in websocket:
            try:
                data = json.loads(message)
                msg_type = data.get("type")

                # ========== НОВЫЙ ШТРИХ (завершённый) ==========
                if msg_type == "draw":
                    stroke = {
                        "userId": users.get(websocket, "unknown"),
                        "points": data.get("points", []),
                        "color": data.get("color", "#FF0000"),
                        "size": data.get("size", 5),
                        "isEraser": data.get("isEraser", False),
                        "timestamp": int(time.time() * 1000)
                    }

                    drawing_history.append(stroke)
                    if len(drawing_history) > MAX_HISTORY:
                        drawing_history.pop(0)

                    # Пересылаем штрих остальным
                    await broadcast(json.dumps({
                        "type": "draw",
                        "stroke": stroke
                    }), exclude=websocket)

                # ========== LIVE-ТОЧКИ (во время рисования) ==========
                elif msg_type == "live":
                    # Просто пересылаем точки, не сохраняем
                    live_msg = json.dumps({
                        "type": "live",
                        "userId": users.get(websocket, "unknown"),
                        "points": data.get("points", []),
                        "color": data.get("color", "#FF0000"),
                        "size": data.get("size", 5),
                        "isEraser": data.get("isEraser", False)
                    })
                    await broadcast(live_msg, exclude=websocket)

                # ========== ОЧИСТКА ==========
                elif msg_type == "clear":
                    drawing_history.clear()
                    await broadcast(json.dumps({"type": "clear"}))

                # ========== ПИНГ ==========
                elif msg_type == "ping":
                    await websocket.send(json.dumps({
                        "type": "pong",
                        "ts": int(time.time() * 1000)
                    }))

            except json.JSONDecodeError:
                print("❌ Ошибка JSON")
            except Exception as e:
                print(f"⚠️ Ошибка обработки: {e}")

    except websockets.exceptions.ConnectionClosed:
        print("🔌 Клиент отключился")
    finally:
        connected_clients.discard(websocket)
        live_buffers.pop(websocket, None)
        nickname = users.pop(websocket, "Аноним")

        await broadcast(json.dumps({
            "type": "user_left",
            "nickname": nickname,
            "users_count": len(connected_clients)
        }))

        print(f"👤 {nickname} отключился (осталось: {len(connected_clients)})")

# ==================== ЗАПУСК ====================
async def main():
    print("🚀 WebSocket сервер на 0.0.0.0:10000")
    async with websockets.serve(
        handler,
        "0.0.0.0",
        10000,
        ping_interval=20,
        ping_timeout=60,
        compression="deflate",     # ← сжатие
        max_size=2**20             # 1 МБ на сообщение
    ):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
