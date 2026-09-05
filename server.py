import asyncio
import websockets
import json
import time

# Хранилище для координат (история рисунка)
drawing_history = []
MAX_HISTORY = 10000  # Максимум 10 000 штрихов

# Храним подключенных пользователей
connected_clients = set()
users = {}  # {websocket: nickname}

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

async def handler(websocket, path):
    """Обработчик подключения"""
    
    # Добавляем в список клиентов
    connected_clients.add(websocket)
    print(f"🔌 Новый клиент подключен (всего: {len(connected_clients)})")
    
    try:
        # Ждем первое сообщение — никнейм
        welcome_msg = await websocket.recv()
        data = json.loads(welcome_msg)
        
        if data.get("type") == "join":
            nickname = data.get("nickname", "Аноним")
            users[websocket] = nickname
            
            # Отправляем новому пользователю историю рисунка
            if drawing_history:
                await websocket.send(json.dumps({
                    "type": "history",
                    "strokes": drawing_history
                }))
            
            # Оповещаем всех о новом пользователе
            await broadcast(json.dumps({
                "type": "user_joined",
                "nickname": nickname,
                "users_count": len(connected_clients)
            }), exclude=websocket)
            
            print(f"👤 {nickname} присоединился")
        
        # Основной цикл приема сообщений
        async for message in websocket:
            try:
                data = json.loads(message)
                msg_type = data.get("type")
                
                if msg_type == "draw":
                    # Получаем координаты
                    stroke = {
                        "userId": users.get(websocket, "unknown"),
                        "points": data.get("points", []),
                        "color": data.get("color", "#FF0000"),
                        "size": data.get("size", 5),
                        "timestamp": int(time.time() * 1000)
                    }
                    
                    # Сохраняем в историю
                    drawing_history.append(stroke)
                    if len(drawing_history) > MAX_HISTORY:
                        drawing_history.pop(0)
                    
                    # Пересылаем ВСЕМ другим клиентам
                    await broadcast(json.dumps({
                        "type": "draw",
                        "stroke": stroke
                    }), exclude=websocket)
                    
                elif msg_type == "clear":
                    # Очистка холста
                    drawing_history.clear()
                    await broadcast(json.dumps({
                        "type": "clear"
                    }))
                    
                elif msg_type == "ping":
                    # Ответ на пинг (держит соединение)
                    await websocket.send(json.dumps({"type": "pong"}))
                    
            except json.JSONDecodeError:
                print("❌ Ошибка парсинга JSON")
            
    except websockets.exceptions.ConnectionClosed:
        print("🔌 Клиент отключился")
    finally:
        # Удаляем клиента
        connected_clients.remove(websocket)
        nickname = users.pop(websocket, "Аноним")
        
        # Оповещаем остальных
        await broadcast(json.dumps({
            "type": "user_left",
            "nickname": nickname,
            "users_count": len(connected_clients)
        }))
        
        print(f"👤 {nickname} отключился (осталось: {len(connected_clients)})")

# Запуск сервера
async def main():
    print("🚀 Запуск WebSocket сервера на 0.0.0.0:10000")
    async with websockets.serve(
        handler, 
        "0.0.0.0",  # Слушаем все интерфейсы
        10000,      # Порт 10000 (стандартный для Render)
        ping_interval=20,  # Пинг каждые 20 секунд
        ping_timeout=60    # Таймаут 60 секунд
    ):
        await asyncio.Future()  # Бесконечно работаем

if __name__ == "__main__":
    asyncio.run(main())