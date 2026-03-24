import asyncio
import json
import os
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import discord
from discord.ext import commands, tasks
from PyQt5 import QtCore, QtWidgets


DATA_FILE = "game_data.json"
DATA_LOCK = threading.Lock()

DAILY_COOLDOWN = 30  # test cooldown in seconds
PLANT_GROW_SECONDS = 60
TAX_INTERVAL = 180  # every 3 minutes
RAID_INTERVAL = 120  # every 2 minutes
RAID_DURATION = 45

START_SEEDS = 5


def now_ts() -> int:
    return int(time.time())


def ensure_data_shape(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw.setdefault("players", {})
    raw.setdefault("farms", {})
    raw.setdefault("meta", {})
    raw["meta"].setdefault("active_raid", None)
    return raw


def load_data() -> Dict[str, Any]:
    with DATA_LOCK:
        if not os.path.exists(DATA_FILE):
            data = ensure_data_shape({})
            save_data(data)
            return data
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}
        return ensure_data_shape(data)


def save_data(data: Dict[str, Any]) -> None:
    with DATA_LOCK:
        tmp = f"{DATA_FILE}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, DATA_FILE)


def get_player(data: Dict[str, Any], user_id: int, username: str) -> Dict[str, Any]:
    uid = str(user_id)
    players = data["players"]
    if uid not in players:
        players[uid] = {
            "username": username,
            "money": 200,
            "seeds": START_SEEDS,
            "plants": [],  # list of {planted_at, grow_seconds}
            "leaves_wet": 0,
            "leaves_dry": 0,
            "joints": 0,
            "daily_last": 0,
            "farm": None,
            "effects": {"relaxed_until": 0, "high_until": 0},
        }
    else:
        players[uid]["username"] = username
        players[uid].setdefault("farm", None)
        players[uid].setdefault("effects", {"relaxed_until": 0, "high_until": 0})
    return players[uid]


def get_farm_bonus(data: Dict[str, Any], user_id: int) -> Dict[str, float]:
    user = get_player(data, user_id, "")
    farm_name = user.get("farm")
    if not farm_name:
        return {"growth": 1.0, "yield": 1.0}

    farm = data["farms"].get(farm_name)
    if not farm:
        user["farm"] = None
        return {"growth": 1.0, "yield": 1.0}

    # Small bonuses for co-op gameplay
    members = max(1, len(farm["members"]))
    growth_bonus = max(0.65, 1.0 - 0.05 * min(members, 7))
    yield_bonus = 1.0 + 0.05 * min(members, 8)
    return {"growth": growth_bonus, "yield": yield_bonus}


def is_effect_active(player: Dict[str, Any], effect: str) -> bool:
    return now_ts() < int(player.get("effects", {}).get(effect, 0))


intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)


@bot.event
async def on_ready() -> None:
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    if not tax_task.is_running():
        tax_task.start()
    if not police_task.is_running():
        police_task.start()


@tasks.loop(seconds=TAX_INTERVAL)
async def tax_task() -> None:
    data = load_data()
    taxed_total = 0
    for uid, player in data["players"].items():
        money = int(player.get("money", 0))
        if money <= 0:
            continue
        rate = random.uniform(0.05, 0.15)
        tax = max(1, int(money * rate))
        player["money"] = max(0, money - tax)
        taxed_total += tax

    # Farm taxes from shared storage
    for farm in data["farms"].values():
        vault = farm["storage"]
        farm_money = int(vault.get("money", 0))
        if farm_money > 0:
            tax = max(1, int(farm_money * 0.1))
            vault["money"] = max(0, farm_money - tax)
            taxed_total += tax

    save_data(data)

    if taxed_total <= 0:
        return

    msg = (
        f"💸 Наркобарон забрал налоги: **{taxed_total}$**. "
        "Сопротивляться бесполезно, он на золотом дирижабле."
    )
    for guild in bot.guilds:
        for channel in guild.text_channels:
            if channel.permissions_for(guild.me).send_messages:
                await channel.send(msg)
                break


@tasks.loop(seconds=RAID_INTERVAL)
async def police_task() -> None:
    data = load_data()
    if data["meta"].get("active_raid"):
        return

    raid_id = str(now_ts())
    data["meta"]["active_raid"] = {
        "id": raid_id,
        "started_at": now_ts(),
        "ends_at": now_ts() + RAID_DURATION,
        "fighters": [],
    }
    save_data(data)

    announcement = (
        "🚨 **РЕЙД ПОЛИЦИИ!** У вас 45 секунд, чтобы отбиться!\n"
        "Пиши `!fightpolice`, чтобы участвовать в обороне теплиц."
    )

    for guild in bot.guilds:
        for channel in guild.text_channels:
            if channel.permissions_for(guild.me).send_messages:
                await channel.send(announcement)
                break

    await asyncio.sleep(RAID_DURATION)

    data = load_data()
    raid = data["meta"].get("active_raid")
    if not raid or raid.get("id") != raid_id:
        return

    fighters: List[str] = raid.get("fighters", [])
    if len(fighters) >= 2:
        for uid in fighters:
            player = data["players"].get(uid)
            if not player:
                continue
            reward = random.randint(20, 80)
            player["money"] += reward
            player["seeds"] += 1
        result = "🛡️ Вы отбились от рейда! Участники получили награды и +1 семечко."
    else:
        # If players fail, random penalties.
        for uid, player in data["players"].items():
            fine = min(player.get("money", 0), random.randint(10, 45))
            player["money"] -= fine
            if player["joints"] > 0 and random.random() < 0.45:
                player["joints"] -= 1
        result = "👮 Рейд провален. Штрафы выписаны, кое-что конфисковано."

    data["meta"]["active_raid"] = None
    save_data(data)

    for guild in bot.guilds:
        for channel in guild.text_channels:
            if channel.permissions_for(guild.me).send_messages:
                await channel.send(result)
                break


@bot.command(name="help")
async def custom_help(ctx: commands.Context) -> None:
    text = (
        "🌿 **Команды Konoplyanka Bot** 🌿\n\n"
        "`!plant` — посадить семена конопли.\n"
        "`!harvest` — собрать листья с созревших кустов.\n"
        "`!dry` — сушить листья и скручивать косяки.\n"
        "`!smoke` — курнуть косяк и получить случайный эффект.\n"
        "`!balance` — показать деньги и ресурсы.\n"
        "`!duel @игрок <ставка>` — дуэль за деньги.\n"
        "`!casino <ставка>` — испытать удачу в казино.\n"
        "`!daily` — ежедневная награда (кд 30 сек).\n"
        "`!fightpolice` — участвовать в обороне от рейда.\n"
        "`!farm create <название>` — создать кооп-ферму.\n"
        "`!farm join <название>` — вступить в кооп-ферму.\n"
        "`!farm info <название>` — информация о ферме.\n"
        "`!help` — показать эту справку."
    )
    await ctx.send(text)


@bot.command()
async def balance(ctx: commands.Context) -> None:
    data = load_data()
    p = get_player(data, ctx.author.id, ctx.author.display_name)
    farm = p.get("farm") or "—"
    msg = (
        f"💰 **{ctx.author.display_name}**, твой баланс:\n"
        f"Деньги: **{p['money']}$**\n"
        f"Семена: **{p['seeds']}**\n"
        f"Растения: **{len(p['plants'])}**\n"
        f"Мокрые листья: **{p['leaves_wet']}**\n"
        f"Сухие листья: **{p['leaves_dry']}**\n"
        f"Косяки: **{p['joints']}**\n"
        f"Ферма: **{farm}**"
    )
    save_data(data)
    await ctx.send(msg)


@bot.command()
async def plant(ctx: commands.Context) -> None:
    data = load_data()
    p = get_player(data, ctx.author.id, ctx.author.display_name)

    if p["seeds"] <= 0:
        await ctx.send("🌱 У тебя нет семян. Выиграй в казино или отбей рейд.")
        return

    bonus = get_farm_bonus(data, ctx.author.id)
    grow_seconds = int(PLANT_GROW_SECONDS * bonus["growth"])

    p["seeds"] -= 1
    p["plants"].append({"planted_at": now_ts(), "grow_seconds": grow_seconds})
    save_data(data)

    await ctx.send(
        f"🪴 {ctx.author.mention} посадил куст! До зрелости примерно **{grow_seconds} сек**."
    )


@bot.command()
async def harvest(ctx: commands.Context) -> None:
    data = load_data()
    p = get_player(data, ctx.author.id, ctx.author.display_name)

    matured = []
    growing = []
    t = now_ts()
    for plant_data in p["plants"]:
        if t - int(plant_data["planted_at"]) >= int(plant_data["grow_seconds"]):
            matured.append(plant_data)
        else:
            growing.append(plant_data)

    if not matured:
        await ctx.send("🌿 Нечего собирать: кусты ещё не созрели.")
        return

    bonus = get_farm_bonus(data, ctx.author.id)
    total_leaves = 0
    for _ in matured:
        base = random.randint(3, 7)
        total_leaves += int(round(base * bonus["yield"]))

    p["plants"] = growing
    p["leaves_wet"] += total_leaves
    save_data(data)

    await ctx.send(
        f"✂️ Собрано **{total_leaves}** мокрых листьев с **{len(matured)}** кустов!"
    )


@bot.command()
async def dry(ctx: commands.Context) -> None:
    data = load_data()
    p = get_player(data, ctx.author.id, ctx.author.display_name)

    if p["leaves_wet"] <= 0 and p["leaves_dry"] < 3:
        await ctx.send("☀️ Сушить нечего. Сначала собери урожай.")
        return

    dried_now = p["leaves_wet"]
    p["leaves_dry"] += dried_now
    p["leaves_wet"] = 0

    rolled = p["leaves_dry"] // 3
    p["leaves_dry"] %= 3
    p["joints"] += rolled

    save_data(data)
    await ctx.send(
        f"🔥 Высушено листьев: **{dried_now}**. Скручено косяков: **{rolled}**."
    )


@bot.command()
async def smoke(ctx: commands.Context) -> None:
    data = load_data()
    p = get_player(data, ctx.author.id, ctx.author.display_name)

    if p["joints"] <= 0:
        await ctx.send("🚬 Пусто. У тебя нет косяков.")
        return

    p["joints"] -= 1
    effect = random.choice(["relax", "laugh", "overhigh"])
    if effect == "relax":
        p["effects"]["relaxed_until"] = now_ts() + 120
        bonus = random.randint(10, 30)
        p["money"] += bonus
        msg = f"😌 Полный дзен. Ты нашёл в диване **{bonus}$**."
    elif effect == "laugh":
        reward = random.randint(1, 2)
        p["seeds"] += reward
        msg = f"😂 Тебя прорвало на смех. Кто-то подарил тебе **{reward}** семян."
    else:
        p["effects"]["high_until"] = now_ts() + 90
        fine = min(p["money"], random.randint(5, 25))
        p["money"] -= fine
        msg = f"🫠 Тебя накрыло. Ты потерял по дороге домой **{fine}$**."

    save_data(data)
    await ctx.send(msg)


@bot.command()
async def daily(ctx: commands.Context) -> None:
    data = load_data()
    p = get_player(data, ctx.author.id, ctx.author.display_name)
    t = now_ts()

    left = DAILY_COOLDOWN - (t - int(p["daily_last"]))
    if left > 0:
        await ctx.send(f"⏳ Ещё рано. До daily осталось **{left} сек**.")
        return

    reward = random.randint(50, 120)
    p["daily_last"] = t
    p["money"] += reward
    p["seeds"] += 1
    save_data(data)
    await ctx.send(f"🎁 Daily: **{reward}$** и 1 семечко!")


@bot.command()
async def casino(ctx: commands.Context, bet: int) -> None:
    if bet <= 0:
        await ctx.send("🎰 Ставка должна быть больше нуля.")
        return

    data = load_data()
    p = get_player(data, ctx.author.id, ctx.author.display_name)
    if p["money"] < bet:
        await ctx.send("💸 Недостаточно денег для такой ставки.")
        return

    roll = random.random()
    if roll < 0.10:
        win = bet * 3
        p["money"] += win
        msg = f"🎉 ДЖЕКПОТ! +**{win}$**"
    elif roll < 0.35:
        win = bet
        p["money"] += win
        msg = f"✨ Повезло! +**{win}$**"
    elif roll < 0.70:
        p["money"] -= bet
        msg = f"😬 Неудача. -**{bet}$**"
    else:
        loss = int(bet * 1.5)
        loss = min(loss, p["money"])
        p["money"] -= loss
        msg = f"💀 Казино безжалостно. -**{loss}$**"

    save_data(data)
    await ctx.send(msg)


@bot.command()
async def duel(ctx: commands.Context, opponent: discord.Member, bet: int) -> None:
    if opponent.bot:
        await ctx.send("🤖 С ботами не дуэлятся.")
        return
    if opponent.id == ctx.author.id:
        await ctx.send("🪞 Дуэль с собой? Сильный ход, но нет.")
        return
    if bet <= 0:
        await ctx.send("⚔️ Ставка должна быть положительной.")
        return

    data = load_data()
    p1 = get_player(data, ctx.author.id, ctx.author.display_name)
    p2 = get_player(data, opponent.id, opponent.display_name)

    if p1["money"] < bet or p2["money"] < bet:
        await ctx.send("💸 У кого-то не хватает денег на ставку.")
        return

    score1 = random.random()
    score2 = random.random()

    if is_effect_active(p1, "relaxed_until"):
        score1 += 0.10
    if is_effect_active(p1, "high_until"):
        score1 -= 0.10

    if is_effect_active(p2, "relaxed_until"):
        score2 += 0.10
    if is_effect_active(p2, "high_until"):
        score2 -= 0.10

    if score1 >= score2:
        p1["money"] += bet
        p2["money"] -= bet
        winner = ctx.author.mention
    else:
        p2["money"] += bet
        p1["money"] -= bet
        winner = opponent.mention

    save_data(data)
    await ctx.send(f"🥊 Дуэль окончена! Победитель: {winner}. Банк: **{bet}$**")


@bot.command()
async def fightpolice(ctx: commands.Context) -> None:
    data = load_data()
    get_player(data, ctx.author.id, ctx.author.display_name)

    raid = data["meta"].get("active_raid")
    if not raid:
        await ctx.send("🕊️ Сейчас рейда нет. Наслаждайся тишиной.")
        return

    uid = str(ctx.author.id)
    if uid in raid["fighters"]:
        await ctx.send("🛡️ Ты уже в обороне.")
        return

    raid["fighters"].append(uid)
    save_data(data)
    await ctx.send(f"🚔 {ctx.author.mention} вступил в бой против рейда!")


@bot.group(invoke_without_command=True)
async def farm(ctx: commands.Context) -> None:
    await ctx.send("Используй: `!farm create <название>`, `!farm join <название>`, `!farm info <название>`")


@farm.command(name="create")
async def farm_create(ctx: commands.Context, *, name: str) -> None:
    farm_key = name.strip().lower()
    if len(farm_key) < 2:
        await ctx.send("🏡 Слишком короткое название фермы.")
        return

    data = load_data()
    p = get_player(data, ctx.author.id, ctx.author.display_name)
    if p.get("farm"):
        await ctx.send("🌾 Ты уже состоишь в ферме. Сначала выйди вручную через JSON (админ-режим).")
        return

    if farm_key in data["farms"]:
        await ctx.send("❌ Такая ферма уже существует.")
        return

    data["farms"][farm_key] = {
        "name": name,
        "owner": str(ctx.author.id),
        "members": [str(ctx.author.id)],
        "storage": {"money": 0, "seeds": 0, "leaves_wet": 0, "leaves_dry": 0, "joints": 0},
        "created_at": now_ts(),
    }
    p["farm"] = farm_key
    save_data(data)
    await ctx.send(f"🏗️ Ферма **{name}** создана! Бонусы коопа уже активны.")


@farm.command(name="join")
async def farm_join(ctx: commands.Context, *, name: str) -> None:
    farm_key = name.strip().lower()
    data = load_data()
    p = get_player(data, ctx.author.id, ctx.author.display_name)

    if p.get("farm"):
        await ctx.send("🌾 Ты уже в ферме.")
        return

    farm_data = data["farms"].get(farm_key)
    if not farm_data:
        await ctx.send("🔎 Ферма не найдена.")
        return

    uid = str(ctx.author.id)
    if uid not in farm_data["members"]:
        farm_data["members"].append(uid)
    p["farm"] = farm_key
    save_data(data)
    await ctx.send(f"🤝 Ты вступил в ферму **{farm_data['name']}**!")


@farm.command(name="info")
async def farm_info(ctx: commands.Context, *, name: str) -> None:
    farm_key = name.strip().lower()
    data = load_data()
    farm_data = data["farms"].get(farm_key)
    if not farm_data:
        await ctx.send("📭 Ферма не найдена.")
        return

    members = []
    for uid in farm_data["members"]:
        player = data["players"].get(uid)
        if player:
            members.append(player.get("username", uid))
        else:
            members.append(uid)

    bonuses = get_farm_bonus(data, int(farm_data["owner"]))
    s = farm_data["storage"]
    msg = (
        f"🏡 **Ферма: {farm_data['name']}**\n"
        f"Участники ({len(members)}): {', '.join(members)}\n"
        f"Бонус роста: x{bonuses['growth']:.2f} к времени\n"
        f"Бонус урожая: x{bonuses['yield']:.2f}\n"
        f"Склад: $ {s['money']}, seeds {s['seeds']}, wet {s['leaves_wet']}, dry {s['leaves_dry']}, joints {s['joints']}"
    )
    await ctx.send(msg)


@dataclass
class BotRuntime:
    thread: Optional[threading.Thread] = None
    loop: Optional[asyncio.AbstractEventLoop] = None
    running: bool = False


class BotControlWidget(QtWidgets.QWidget):
    def __init__(self) -> None:
        super().__init__()
        self.runtime = BotRuntime()
        self.last_error = False
        self.setWindowTitle("Konoplyanka Discord Bot")
        self.setMinimumWidth(480)

        layout = QtWidgets.QVBoxLayout(self)

        self.token_input = QtWidgets.QLineEdit()
        self.token_input.setPlaceholderText("Discord Bot Token")
        self.token_input.setEchoMode(QtWidgets.QLineEdit.Password)

        self.guild_input = QtWidgets.QLineEdit()
        self.guild_input.setPlaceholderText("Guild ID (например 1234567890)")

        self.start_button = QtWidgets.QPushButton("Запустить бота")
        self.stop_button = QtWidgets.QPushButton("Остановить бота")
        self.stop_button.setEnabled(False)

        self.status = QtWidgets.QLabel("Статус: бот не запущен")

        layout.addWidget(QtWidgets.QLabel("Токен:"))
        layout.addWidget(self.token_input)
        layout.addWidget(QtWidgets.QLabel("Guild ID:"))
        layout.addWidget(self.guild_input)
        layout.addWidget(self.start_button)
        layout.addWidget(self.stop_button)
        layout.addWidget(self.status)

        self.start_button.clicked.connect(self.start_bot)
        self.stop_button.clicked.connect(self.stop_bot)

        self.status_timer = QtCore.QTimer(self)
        self.status_timer.timeout.connect(self.refresh_status)
        self.status_timer.start(1000)

    def set_status(self, text: str) -> None:
        self.status.setText(f"Статус: {text}")

    def refresh_status(self) -> None:
        if self.runtime.running:
            self.set_status("бот запущен ✅")
        elif self.runtime.thread is not None:
            if self.runtime.thread.is_alive():
                self.set_status("бот запускается")
            elif self.last_error:
                self.set_status("Ошибка запуска")
            else:
                self.set_status("бот не запущен")

    def start_bot(self) -> None:
        token = self.token_input.text().strip()
        guild_id_text = self.guild_input.text().strip()

        if not token:
            self.set_status("Ошибка запуска: пустой токен")
            return

        if guild_id_text:
            try:
                int(guild_id_text)
            except ValueError:
                self.set_status("Ошибка запуска: Guild ID должен быть числом")
                return

        if self.runtime.running or (self.runtime.thread and self.runtime.thread.is_alive()):
            self.set_status("бот уже запускается/работает")
            return

        self.last_error = False
        self.set_status("бот запускается")

        def runner() -> None:
            loop = asyncio.new_event_loop()
            self.runtime.loop = loop
            asyncio.set_event_loop(loop)
            try:
                self.runtime.running = True
                loop.run_until_complete(bot.start(token))
            except Exception as exc:
                print(f"Bot start error: {exc}")
                self.runtime.running = False
                self.last_error = True
            finally:
                self.runtime.running = False
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                try:
                    loop.run_until_complete(asyncio.sleep(0))
                except Exception:
                    pass
                loop.stop()
                loop.close()

        self.runtime.thread = threading.Thread(target=runner, daemon=True)
        self.runtime.thread.start()

        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)

    def stop_bot(self) -> None:
        if not self.runtime.loop or not self.runtime.running:
            self.set_status("бот не запущен")
            self.start_button.setEnabled(True)
            self.stop_button.setEnabled(False)
            return

        async def shutdown() -> None:
            await bot.close()

        try:
            asyncio.run_coroutine_threadsafe(shutdown(), self.runtime.loop)
            self.runtime.running = False
            self.set_status("бот не запущен")
        except Exception:
            self.set_status("Ошибка запуска")

        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)


def main() -> None:
    _ = load_data()  # init file if needed
    app = QtWidgets.QApplication([])
    widget = BotControlWidget()
    widget.show()
    app.exec_()


if __name__ == "__main__":
    main()
