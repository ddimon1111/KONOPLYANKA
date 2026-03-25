import asyncio
import json
import os
import random
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional

import discord
import requests
from discord.ext import commands, tasks
from PyQt5 import QtCore, QtGui, QtWidgets

# ================================================================
# Configuration
# ================================================================
DATA_FILE = "game_data.json"
SERVER_URL = "http://127.0.0.1:5001"
BOT_PREFIX = "!"
LOG_FILE = "bot_events.log"

PLANT_COOLDOWN = 1800
DAILY_COOLDOWN = 86400
SMOKE_DURATION = 1800
PLANT_BASE_GROWTH = 1800
AUTO_INCOME_INTERVAL = 60
TAX_INTERVAL = 1800
RAID_INTERVAL = 1800

BASE_JOINT_PRICE = 35
XP_PER_ACTION = 8
XP_LEVEL_SCALE = 120

# Contraband configuration: duration in seconds, min/max reward
CONTRABAND_COUNTRIES: Dict[str, Dict[str, int]] = {
    "china": {"duration": 3000, "min": 150, "max": 280},
    "russia": {"duration": 2400, "min": 120, "max": 240},
    "ukraine": {"duration": 2100, "min": 110, "max": 220},
    "canada": {"duration": 1800, "min": 95, "max": 180},
    "usa": {"duration": 1680, "min": 90, "max": 170},
    "brazil": {"duration": 2700, "min": 130, "max": 260},
    "venezuela": {"duration": 3300, "min": 170, "max": 320},
}

RARITY_TABLE: List[Dict[str, Any]] = [
    {"name": "common", "weight": 55, "yield_min": 2, "yield_max": 4},
    {"name": "rare", "weight": 28, "yield_min": 4, "yield_max": 7},
    {"name": "epic", "weight": 13, "yield_min": 7, "yield_max": 11},
    {"name": "legendary", "weight": 4, "yield_min": 11, "yield_max": 16},
]

BADGE_SHOP: List[Dict[str, Any]] = [
    {"id": 1, "name": "🌱 Новичок Фермы", "price_met": 1},
    {"id": 2, "name": "💧 Мастер Полива", "price_met": 1},
    {"id": 3, "name": "🚬 Роллер", "price_met": 1},
    {"id": 4, "name": "💸 Капиталист", "price_met": 2},
    {"id": 5, "name": "🛡️ Защитник", "price_met": 2},
    {"id": 6, "name": "🧪 Химик", "price_met": 2},
    {"id": 7, "name": "🏡 Фермер PRO", "price_met": 2},
    {"id": 8, "name": "🎰 Казино Король", "price_met": 3},
    {"id": 9, "name": "🧳 Контрабандист", "price_met": 3},
    {"id": 10, "name": "⚔️ Дуэлянт", "price_met": 3},
    {"id": 11, "name": "🕶️ Картель Босс", "price_met": 4},
    {"id": 12, "name": "🏆 Лидер", "price_met": 4},
    {"id": 13, "name": "💎 VIP", "price_met": 5},
    {"id": 14, "name": "🌌 Легенда", "price_met": 6},
    {"id": 15, "name": "👑 Абсолют", "price_met": 8},
]

WEAPON_STATS: Dict[str, Dict[str, float]] = {
    "glock": {"price": 800, "raid_bonus": 0.05},
    "berretta": {"price": 900, "raid_bonus": 0.07},
    "tec": {"price": 1200, "raid_bonus": 0.10},
    "db": {"price": 1500, "raid_bonus": 0.12},
    "ak47": {"price": 3000, "raid_bonus": 0.20},
    "m16": {"price": 3500, "raid_bonus": 0.25},
}

BLACKMARKET_RARE = {
    "armor_plate": 5000,
    "thermal_scope": 4500,
}

DATA_LOCK = threading.RLock()
BOT_STATUS_CALLBACK = None
ALLOWED_GUILD_ID: Optional[int] = None

I18N: Dict[str, Dict[str, str]] = {
    "ru": {
        "about": "Я не поддерживаю наркотики, это просто игра для развлечения.",
        "menu_title": "🌿 Меню экономики",
        "settings": "⚙️ Настройки",
        "language_set_ru": "Язык изменён на русский 🇷🇺",
        "language_set_en": "Language switched to English 🇬🇧",
        "no_plants_water": "💧 У тебя нет растений для полива.",
        "water_done": "💧 Полито растений: **{count}**.",
        "care_done": "🧤 Уход выполнен. Урожайность повышена.",
        "inventory": "🎒 Инвентарь",
    },
    "en": {
        "about": "I do not support drugs, this game is just for fun.",
        "menu_title": "🌿 Economy Menu",
        "settings": "⚙️ Settings",
        "language_set_ru": "Язык изменён на русский 🇷🇺",
        "language_set_en": "Language switched to English 🇬🇧",
        "no_plants_water": "💧 You have no plants to water.",
        "water_done": "💧 Watered plants: **{count}**.",
        "care_done": "🧤 Plant care done. Yield improved.",
        "inventory": "🎒 Inventory",
    },
}

# Ukrainian locale fallback (full map via ru copy to prevent missing keys / KeyError).
I18N["ua"] = dict(I18N["ru"])
I18N["ua"].update(
    {
        "about": "Я не підтримую наркотики, це просто гра для розваги.",
        "menu_title": "🌿 Меню економіки",
        "settings": "⚙️ Налаштування",
        "inventory": "🎒 Інвентар",
    }
)


# ================================================================
# Utility
# ================================================================
def now_ts() -> int:
    return int(time.time())


def log_event(event_type: str, user: str, details: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {event_type} | {user} | {details}\n"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def day_bucket(ts: Optional[int] = None) -> int:
    ts = now_ts() if ts is None else ts
    return ts // 86400


def active_weapon_bonus(player: Dict[str, Any]) -> float:
    weapon = player.get("active_weapon")
    if not weapon or now_ts() > int(player.get("weapon_until", 0)):
        return 0.0
    return float(WEAPON_STATS.get(weapon, {}).get("raid_bonus", 0.0))


def active_meth_bonus(player: Dict[str, Any]) -> float:
    return 0.30 if now_ts() < int(player.get("meth_until", 0)) else 0.0


def clamp_player(player: Dict[str, Any]) -> None:
    player["money"] = max(0, int(player.get("money", 0)))
    player["influence"] = max(-100, min(100, int(player.get("influence", 0))))


def weighted_rarity() -> Dict[str, Any]:
    items = [entry["name"] for entry in RARITY_TABLE]
    weights = [entry["weight"] for entry in RARITY_TABLE]
    choice = random.choices(items, weights=weights, k=1)[0]
    return next(entry for entry in RARITY_TABLE if entry["name"] == choice)


def xp_to_next(level: int) -> int:
    return XP_LEVEL_SCALE + (level - 1) * 45


def ensure_shape(data: Dict[str, Any]) -> Dict[str, Any]:
    data.setdefault("players", {})
    data.setdefault("farms", {})
    data.setdefault("cartels", {})
    data.setdefault("tournaments", {})
    data.setdefault("meta", {})
    data.setdefault("auctions", {})
    data["meta"].setdefault("active_raid", None)
    data["meta"].setdefault("next_auction_id", 1)
    data["meta"].setdefault("next_tournament_id", 1)
    data["meta"].setdefault("next_duel_id", 1)
    data["meta"].setdefault("duels", {})
    return data


def default_player(username: str) -> Dict[str, Any]:
    return {
        "username": username,
        "money": 400,
        "bank": 0,
        "meth": 0,
        "salt": 0,
        "xp": 0,
        "level": 1,
        "leaves_wet": 0,
        "leaves_dry": 0,
        "joints": 0,
        "plants": [],
        "land_plots": 3,
        "cooldowns": {
            "plant": 0,
            "daily": 0,
        },
        "upgrades": {
            "farm_level": 0,
            "growth_level": 0,
            "shield_level": 0,
        },
        "shield_until": 0,
        "smoke_until": 0,
        "auto_watering": False,
        "fertilizers": 0,
        "chemicals": 0,
        "inventory": {"lamps": 0},
        "language": "ru",
        "cartel": None,
        "reputation": {"street": 0, "police": 0},
        "badges": [],
        "energy": 100,
        "notify_dm": True,
        "weapons": {},
        "active_weapon": None,
        "weapon_until": 0,
        "meth_until": 0,
        "raid": {"day": 0, "count": 0, "last_ts": 0},
        "loan": {"amount": 0, "updated_at": 0},
        "influence": 0,
        "workers": 0,
        "workers_active": True,
        "last_salary_day": day_bucket(),
        "raid_targets": {},
        "transfer": {"day": day_bucket(), "sent": 0},
        "farm": None,
        "contraband": None,
    }


def load_data() -> Dict[str, Any]:
    try:
        resp = requests.get(f"{SERVER_URL}/get", timeout=2.0)
        if resp.ok:
            return ensure_shape(resp.json())
    except Exception as e:
        log_event("HTTP_WARN", "system", f"GET /get failed: {e}")
    # Fallback: local file read (stability mode if server is down)
    with DATA_LOCK:
        if not os.path.exists(DATA_FILE):
            data = ensure_shape({})
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return data
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}
        return ensure_shape(data)


def save_data(data: Dict[str, Any]) -> None:
    data = ensure_shape(data)
    try:
        requests.post(f"{SERVER_URL}/save", json=data, timeout=2.0)
        return
    except Exception as e:
        log_event("HTTP_WARN", "system", f"POST /save failed: {e}")
    # Fallback local save
    with DATA_LOCK:
        tmp = f"{DATA_FILE}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, DATA_FILE)


def update_player_http(user_id: str, updates: Dict[str, Any]) -> None:
    try:
        requests.post(f"{SERVER_URL}/update_player", json={"user_id": str(user_id), "updates": updates}, timeout=2.0)
    except Exception as e:
        log_event("HTTP_WARN", str(user_id), f"POST /update_player failed: {e}")


def get_player(data: Dict[str, Any], user: discord.abc.User) -> Dict[str, Any]:
    uid = str(user.id)
    players = data["players"]
    if uid not in players:
        players[uid] = default_player(user.name)
    players[uid]["username"] = user.name
    players[uid].setdefault("farm", None)
    players[uid].setdefault("contraband", None)
    players[uid].setdefault("land_plots", 3)
    players[uid].setdefault("auto_watering", False)
    players[uid].setdefault("fertilizers", 0)
    players[uid].setdefault("chemicals", 0)
    players[uid].setdefault("inventory", {"lamps": 0})
    players[uid].setdefault("language", "ru")
    players[uid].setdefault("cartel", None)
    players[uid].setdefault("reputation", {"street": 0, "police": 0})
    if "met" in players[uid] and "meth" not in players[uid]:
        players[uid]["meth"] = players[uid].get("met", 0)
    players[uid].setdefault("meth", 0)
    players[uid].setdefault("badges", [])
    players[uid].setdefault("energy", 100)
    players[uid].setdefault("notify_dm", True)
    players[uid].setdefault("weapons", {})
    players[uid].setdefault("active_weapon", None)
    players[uid].setdefault("weapon_until", 0)
    players[uid].setdefault("meth_until", 0)
    players[uid].setdefault("raid", {"day": 0, "count": 0, "last_ts": 0})
    players[uid].setdefault("loan", {"amount": 0, "updated_at": 0})
    players[uid].setdefault("influence", 0)
    players[uid].setdefault("workers", 0)
    players[uid].setdefault("workers_active", True)
    players[uid].setdefault("last_salary_day", day_bucket())
    players[uid].setdefault("raid_targets", {})
    players[uid].setdefault("transfer", {"day": day_bucket(), "sent": 0})
    return players[uid]


def tr(player: Dict[str, Any], key: str, **kwargs: Any) -> str:
    lang = player.get("language", "ru")
    table = I18N.get(lang, I18N["ru"])
    template = table.get(key, I18N["ru"].get(key, key))
    return template.format(**kwargs)


def add_xp(player: Dict[str, Any], amount: int) -> Optional[str]:
    player["xp"] += amount
    leveled = False
    while player["xp"] >= xp_to_next(player["level"]):
        player["xp"] -= xp_to_next(player["level"])
        player["level"] += 1
        player["money"] += 80 + player["level"] * 10
        leveled = True
    if leveled:
        return f"🎉 Level up! You are now level **{player['level']}**."
    return None


def cooldown_left(player: Dict[str, Any], key: str, seconds: int) -> int:
    last = int(player["cooldowns"].get(key, 0))
    rem = seconds - (now_ts() - last)
    return max(0, rem)


def growth_seconds_for_player(player: Dict[str, Any]) -> int:
    growth_level = int(player["upgrades"]["growth_level"])
    reduction = int(PLANT_BASE_GROWTH * min(0.75, growth_level * 0.08))
    has_energy = int(player.get("energy", 0)) > 0
    lamp_reduction = min(8, int(player.get("inventory", {}).get("lamps", 0))) if has_energy else 0
    boosted = now_ts() < int(player.get("smoke_until", 0))
    smoke_bonus = 5 if boosted else 0
    return max(6, PLANT_BASE_GROWTH - reduction - smoke_bonus - lamp_reduction)


def farm_upgrade_cost(level: int) -> int:
    return 1000 + 500 * level


def growth_upgrade_cost(level: int) -> int:
    return 900 + 450 * level


def shield_upgrade_cost(level: int) -> int:
    return 800 + 400 * level


def lamp_cost(current_lamps: int) -> int:
    return 300 + current_lamps * 150


def resolve_contraband(player: Dict[str, Any]) -> Optional[str]:
    mission = player.get("contraband")
    if not mission:
        return None
    if now_ts() < int(mission["ends_at"]):
        left = int(mission["ends_at"]) - now_ts()
        return f"🧳 Mission still running to **{mission['country']}**. {left}s left."

    reward = int(mission["reward"])
    risk_roll = random.random()
    if risk_roll < 0.18:
        penalty = min(player["money"], max(40, reward // 3))
        player["money"] -= penalty
        player["contraband"] = None
        return f"🚓 Contraband busted. You lost **${penalty}**."

    player["money"] += reward
    salt_gain = max(1, reward // 60)
    player["salt"] += salt_gain
    player["contraband"] = None
    return f"✅ Contraband completed: +**${reward}**, +**{salt_gain} salt**."


async def safe_interaction_reply(
    interaction: discord.Interaction,
    *,
    content: Optional[str] = None,
    embed: Optional[discord.Embed] = None,
    ephemeral: bool = True,
) -> None:
    """Always acknowledge interactions safely to avoid 'This interaction failed'."""
    if interaction.response.is_done():
        await interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
    else:
        await interaction.response.send_message(content=content, embed=embed, ephemeral=ephemeral)


# ================================================================
# Discord bot setup
# ================================================================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=BOT_PREFIX, intents=intents, help_command=None)


# ================================================================
# Core game actions
# ================================================================
def action_plant(player: Dict[str, Any]) -> str:
    if len(player["plants"]) >= int(player.get("land_plots", 3)):
        return "🧱 All land plots are occupied. Upgrade farm for more plots."

    left = cooldown_left(player, "plant", PLANT_COOLDOWN)
    if left > 0:
        return f"⏳ Plant cooldown active: **{left}s** left."

    rarity = weighted_rarity()
    growth_time = growth_seconds_for_player(player)
    speed_bonus = 0
    if player.get("fertilizers", 0) > 0:
        player["fertilizers"] -= 1
        speed_bonus += 4
    if player.get("chemicals", 0) > 0:
        player["chemicals"] -= 1
        speed_bonus += 6

    final_growth = max(6, growth_time - speed_bonus)
    plant = {
        "rarity": rarity["name"],
        "yield_min": rarity["yield_min"],
        "yield_max": rarity["yield_max"],
        "planted_at": now_ts(),
        "ready_at": now_ts() + final_growth,
        "last_watered": 0,
        "care": 0,
    }
    player["plants"].append(plant)
    player["cooldowns"]["plant"] = now_ts()
    add_xp(player, XP_PER_ACTION)
    return (
        f"🌱 Planted **{rarity['name']}** seed. "
        f"Ready in **{final_growth}s**."
    )


def action_harvest(player: Dict[str, Any]) -> str:
    ready = [
        p for p in player["plants"]
        if now_ts() >= int(p["ready_at"]) and (now_ts() - int(p.get("last_watered", 0)) <= 180)
    ]
    if not ready:
        return "🌿 No mature plants yet."

    farm_level = int(player["upgrades"]["farm_level"])
    efficiency = 1.0 + farm_level * 0.15
    total = 0
    for plant in ready:
        base = random.randint(int(plant["yield_min"]), int(plant["yield_max"]))
        care_bonus = 1 + min(0.35, float(plant.get("care", 0)) * 0.05)
        total += max(1, int(base * efficiency * care_bonus))
        player["plants"].remove(plant)

    player["leaves_wet"] += total
    add_xp(player, XP_PER_ACTION + len(ready) * 2)
    return f"✂️ Harvested **{len(ready)}** plants for **{total} wet leaves**."


def action_dry(player: Dict[str, Any]) -> str:
    if player["leaves_wet"] <= 0:
        return "🍃 No wet leaves to dry."
    amount = player["leaves_wet"]
    player["leaves_wet"] = 0
    player["leaves_dry"] += amount
    add_xp(player, XP_PER_ACTION)
    return f"☀️ Dried **{amount}** leaves."


def action_roll(player: Dict[str, Any]) -> str:
    if player["leaves_dry"] < 2:
        return "🧻 Need at least 2 dry leaves per joint."
    joints = player["leaves_dry"] // 2
    player["leaves_dry"] -= joints * 2
    player["joints"] += joints
    add_xp(player, XP_PER_ACTION)
    return f"🚬 Rolled **{joints} joints**."


def action_smoke(player: Dict[str, Any]) -> str:
    if player["joints"] <= 0:
        return "🚭 You have no joints to smoke."
    player["joints"] -= 1
    player["smoke_until"] = now_ts() + SMOKE_DURATION
    bonus = random.randint(20, 80)
    player["money"] += bonus
    add_xp(player, XP_PER_ACTION)
    return (
        f"💨 You smoked a joint. Boost active for **{SMOKE_DURATION}s** "
        f"(faster growth), and found **${bonus}** while vibing."
    )


def action_water(player: Dict[str, Any]) -> str:
    """Water all non-recently-watered plants for this player."""
    if not player["plants"]:
        return tr(player, "no_plants_water")
    watered = 0
    for plant in player["plants"]:
        if now_ts() - int(plant.get("last_watered", 0)) > 60:
            plant["last_watered"] = now_ts()
            watered += 1
    if watered == 0:
        return "💧 Растения уже недавно были политы."
    add_xp(player, 6)
    return tr(player, "water_done", count=watered)


def action_care(player: Dict[str, Any]) -> str:
    """Increase care meter for all active plants to improve yield."""
    if not player["plants"]:
        return "🧤 You have no plants to care for."
    for plant in player["plants"]:
        plant["care"] = min(7, int(plant.get("care", 0)) + 1)
    add_xp(player, 6)
    return tr(player, "care_done")


def action_balance(player: Dict[str, Any]) -> discord.Embed:
    embed = discord.Embed(title="💼 Economy Balance", color=discord.Color.green())
    embed.add_field(name="Cash", value=f"${player['money']}", inline=True)
    embed.add_field(name="Bank", value=f"${player['bank']}", inline=True)
    embed.add_field(name="Мёт", value=str(player.get("meth", 0)), inline=True)
    embed.add_field(name="Salt", value=str(player["salt"]), inline=True)
    embed.add_field(name="Wet leaves", value=str(player["leaves_wet"]), inline=True)
    embed.add_field(name="Dry leaves", value=str(player["leaves_dry"]), inline=True)
    embed.add_field(name="Joints", value=str(player["joints"]), inline=True)
    embed.add_field(name="Level", value=str(player["level"]), inline=True)
    embed.add_field(name="XP", value=f"{player['xp']}/{xp_to_next(player['level'])}", inline=True)
    embed.add_field(name="Shield", value=f"Lv {player['upgrades']['shield_level']}", inline=True)
    embed.add_field(name="Land plots", value=str(player.get("land_plots", 3)), inline=True)
    embed.add_field(name="Auto-water", value="On" if player.get("auto_watering") else "Off", inline=True)
    embed.add_field(name="Fertilizers", value=str(player.get("fertilizers", 0)), inline=True)
    embed.add_field(name="Chemicals", value=str(player.get("chemicals", 0)), inline=True)
    embed.add_field(name="Badges", value=str(len(player.get("badges", []))), inline=True)
    return embed


# ================================================================
# UI Views
# ================================================================
class GameMenuView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    async def _perform(self, interaction: discord.Interaction, action: str) -> None:
        data = load_data()
        player = get_player(data, interaction.user)

        if action == "plant":
            msg = action_plant(player)
            save_data(data)
            await safe_interaction_reply(interaction, content=msg)
            return
        if action == "harvest":
            msg = action_harvest(player)
            save_data(data)
            await safe_interaction_reply(interaction, content=msg)
            return
        if action == "smoke":
            msg = action_smoke(player)
            save_data(data)
            await safe_interaction_reply(interaction, content=msg)
            return
        if action == "balance":
            embed = action_balance(player)
            save_data(data)
            await safe_interaction_reply(interaction, embed=embed)
            return
        if action == "upgrade":
            save_data(data)
            await safe_interaction_reply(
                interaction,
                content="🛒 Upgrade Shop",
                ephemeral=True,
            )
            await interaction.followup.send(
                content="Choose an upgrade:",
                view=UpgradeShopView(),
                ephemeral=True,
            )
            return
        if action == "about":
            await safe_interaction_reply(
                interaction,
                content=tr(player, "about"),
            )
            return
        if action == "settings":
            await safe_interaction_reply(
                interaction,
                content=tr(player, "settings"),
                ephemeral=True,
            )
            await interaction.followup.send(
                content="Выберите язык / Choose language:",
                view=SettingsView(),
                ephemeral=True,
            )
            return
        if action == "use":
            save_data(data)
            await safe_interaction_reply(
                interaction,
                content="Выбери предмет для использования:",
                view=UseItemView(),
                ephemeral=True,
            )
            return

    @discord.ui.button(label="Plant", style=discord.ButtonStyle.success)
    async def plant_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._perform(interaction, "plant")

    @discord.ui.button(label="Harvest", style=discord.ButtonStyle.primary)
    async def harvest_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._perform(interaction, "harvest")

    @discord.ui.button(label="Smoke", style=discord.ButtonStyle.secondary)
    async def smoke_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._perform(interaction, "smoke")

    @discord.ui.button(label="Balance", style=discord.ButtonStyle.blurple)
    async def balance_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._perform(interaction, "balance")

    @discord.ui.button(label="Upgrade Shop", style=discord.ButtonStyle.danger)
    async def upgrade_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._perform(interaction, "upgrade")

    @discord.ui.button(label="About", style=discord.ButtonStyle.gray)
    async def about_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._perform(interaction, "about")

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.gray)
    async def settings_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._perform(interaction, "settings")

    @discord.ui.button(label="Use", style=discord.ButtonStyle.success, row=1)
    async def use_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._perform(interaction, "use")


class UpgradeShopView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    async def _buy(self, interaction: discord.Interaction, item: str) -> None:
        data = load_data()
        player = get_player(data, interaction.user)
        upgrades = player["upgrades"]

        if item == "farm":
            level = int(upgrades["farm_level"])
            price = farm_upgrade_cost(level)
            if player["money"] < price:
                await safe_interaction_reply(interaction, content=f"❌ Need ${price} for Farm upgrade.")
                return
            player["money"] -= price
            upgrades["farm_level"] += 1
            player["land_plots"] = int(player.get("land_plots", 3)) + 1
            add_xp(player, 15)
            save_data(data)
            await safe_interaction_reply(
                interaction,
                content=(
                    f"✅ Farm upgraded to Lv {upgrades['farm_level']} for ${price}. "
                    f"Land plots: {player['land_plots']}"
                ),
            )
            return

        if item == "growth":
            level = int(upgrades["growth_level"])
            price = growth_upgrade_cost(level)
            if player["money"] < price:
                await safe_interaction_reply(interaction, content=f"❌ Need ${price} for Growth boost.")
                return
            player["money"] -= price
            upgrades["growth_level"] += 1
            add_xp(player, 15)
            save_data(data)
            await safe_interaction_reply(
                interaction,
                content=f"✅ Growth upgraded to Lv {upgrades['growth_level']} for ${price}.",
            )
            return

        if item == "shield":
            level = int(upgrades["shield_level"])
            price = shield_upgrade_cost(level)
            if player["money"] < price:
                await safe_interaction_reply(interaction, content=f"❌ Need ${price} for Shield.")
                return
            player["money"] -= price
            upgrades["shield_level"] += 1
            duration = 90 + 45 * upgrades["shield_level"]
            player["shield_until"] = now_ts() + duration
            add_xp(player, 15)
            save_data(data)
            await safe_interaction_reply(
                interaction,
                content=(
                    f"🛡️ Shield upgraded to Lv {upgrades['shield_level']} for ${price}. "
                    f"Protection active for {duration}s."
                ),
            )
            return

        if item == "lamp":
            current = int(player.get("inventory", {}).get("lamps", 0))
            price = lamp_cost(current)
            if player["money"] < price:
                await safe_interaction_reply(interaction, content=f"❌ Нужно ${price} для лампы.")
                return
            player["money"] -= price
            player["inventory"]["lamps"] = current + 1
            save_data(data)
            await safe_interaction_reply(
                interaction,
                content=f"💡 Куплена лампа. Теперь ламп: {player['inventory']['lamps']} (цена ${price}).",
            )
            return

    @discord.ui.button(label="Farm Upgrade", style=discord.ButtonStyle.success)
    async def farm_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._buy(interaction, "farm")

    @discord.ui.button(label="Growth Boost", style=discord.ButtonStyle.primary)
    async def growth_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._buy(interaction, "growth")

    @discord.ui.button(label="Shield", style=discord.ButtonStyle.danger)
    async def shield_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._buy(interaction, "shield")

    @discord.ui.button(label="Lamp", style=discord.ButtonStyle.secondary)
    async def lamp_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._buy(interaction, "lamp")


class SettingsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    @discord.ui.button(label="Русский", style=discord.ButtonStyle.primary)
    async def ru_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        data = load_data()
        player = get_player(data, interaction.user)
        player["language"] = "ru"
        save_data(data)
        await safe_interaction_reply(interaction, content=tr(player, "language_set_ru"))

    @discord.ui.button(label="English", style=discord.ButtonStyle.success)
    async def en_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        data = load_data()
        player = get_player(data, interaction.user)
        player["language"] = "en"
        save_data(data)
        await safe_interaction_reply(interaction, content=tr(player, "language_set_en"))

    @discord.ui.button(label="Українська", style=discord.ButtonStyle.secondary)
    async def ua_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        data = load_data()
        player = get_player(data, interaction.user)
        player["language"] = "ua"
        save_data(data)
        await safe_interaction_reply(interaction, content="Мову змінено на українську 🇺🇦")


class UseItemView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    async def _use(self, interaction: discord.Interaction, item: str) -> None:
        # directly run use logic without duplicating command routing
        data = load_data()
        player = get_player(data, interaction.user)
        inv = player.setdefault("inventory", {})
        weapons = player.setdefault("weapons", {})

        msg = None
        if item in {"lamp", "fert", "chem", "salt", "meth"} or item in WEAPON_STATS:
            # lightweight mirror of !use to keep button action deterministic.
            if item == "lamp" and inv.get("lamps", 0) > 0:
                player["energy"] = min(200, int(player.get("energy", 100)) + 30)
                msg = f"💡 Энергия: {player['energy']}"
            elif item == "fert" and player.get("fertilizers", 0) > 0:
                player["fertilizers"] -= 1
                player["upgrades"]["growth_level"] = player["upgrades"].get("growth_level", 0) + 1
                msg = "🧪 Удобрение использовано."
            elif item == "chem" and player.get("chemicals", 0) > 0:
                player["chemicals"] -= 1
                player["smoke_until"] = now_ts() + 1200
                msg = "⚗️ Химикат использован."
            elif item == "salt" and player.get("salt", 0) > 0:
                player["salt"] -= 1
                player["money"] += 50
                msg = "🤧 Соль использована."
            elif item == "meth" and player.get("meth", 0) > 0:
                player["meth"] -= 1
                player["meth_until"] = now_ts() + 1800
                player["influence"] = min(100, int(player.get("influence", 0)) + 5)
                if random.random() < 0.10:
                    fine = min(player["money"], 120)
                    player["money"] -= fine
                    msg = f"🧪 Meth использован. Риск сработал, штраф ${fine}."
                else:
                    msg = "🧪 Meth использован: +30% к рейду на 30 минут."
            elif item in WEAPON_STATS and weapons.get(item, 0) > 0:
                player["active_weapon"] = item
                player["weapon_until"] = now_ts() + 1800
                weapons[item] -= 1
                if weapons[item] <= 0:
                    weapons.pop(item, None)
                msg = f"🔫 {item} активирован."
        if not msg:
            msg = "Предмет недоступен."
        save_data(data)
        await safe_interaction_reply(interaction, content=msg, ephemeral=True)

    @discord.ui.button(label="Lamp", style=discord.ButtonStyle.secondary)
    async def use_lamp(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._use(interaction, "lamp")

    @discord.ui.button(label="Fert", style=discord.ButtonStyle.secondary)
    async def use_fert(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._use(interaction, "fert")

    @discord.ui.button(label="Chem", style=discord.ButtonStyle.secondary)
    async def use_chem(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._use(interaction, "chem")

    @discord.ui.button(label="Salt", style=discord.ButtonStyle.primary, row=1)
    async def use_salt(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._use(interaction, "salt")

    @discord.ui.button(label="Met", style=discord.ButtonStyle.primary, row=1)
    async def use_met(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._use(interaction, "meth")


# ================================================================
# Commands
# ================================================================
@bot.event
async def on_ready() -> None:
    print(f"Logged in as {bot.user} ({bot.user.id})")
    if BOT_STATUS_CALLBACK:
        BOT_STATUS_CALLBACK("Bot online ✅")
    if not auto_income_loop.is_running():
        auto_income_loop.start()
    if not tax_loop.is_running():
        tax_loop.start()
    if not police_loop.is_running():
        police_loop.start()


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot:
        return
    if ALLOWED_GUILD_ID is not None:
        if message.guild is None or int(message.guild.id) != int(ALLOWED_GUILD_ID):
            return
    await bot.process_commands(message)


@bot.command(name="about")
async def about_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    save_data(data)
    await ctx.send(tr(player, "about"))


@bot.command(name="menu")
async def menu_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    save_data(data)
    tree = (
        "🌿 МЕНЮ\n"
        "├ 💰 Экономика\n"
        "├ 🤝 Кооп\n"
        "├ 💨 Кайф\n"
        "├ ⚔️ PvP\n"
        "├ 🛒 Магазины\n"
        "├ 🎒 Инвентарь\n"
        "└ ⚙️ Настройки"
    )
    await ctx.send(f"{tr(player, 'menu_title')}\n{tree}", view=GameMenuView())


@bot.command(name="plant")
async def plant_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    msg = action_plant(player)
    save_data(data)
    await ctx.send(msg)


@bot.command(name="harvest")
async def harvest_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    msg = action_harvest(player)
    save_data(data)
    await ctx.send(msg)


@bot.command(name="dry")
async def dry_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    msg = action_dry(player)
    save_data(data)
    await ctx.send(msg)


@bot.command(name="roll")
async def roll_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    msg = action_roll(player)
    save_data(data)
    await ctx.send(msg)


@bot.command(name="smoke")
async def smoke_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    msg = action_smoke(player)
    save_data(data)
    await ctx.send(msg)


@bot.command(name="water")
async def water_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    msg = action_water(player)
    save_data(data)
    await ctx.send(msg)


@bot.command(name="care")
async def care_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    msg = action_care(player)
    save_data(data)
    await ctx.send(msg)


@bot.command(name="autowater")
async def autowater_cmd(ctx: commands.Context, mode: str) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    mode = mode.lower().strip()
    if mode not in {"on", "off"}:
        await ctx.send("Usage: !autowater on|off")
        return
    if mode == "on":
        upkeep = 200
        if player["money"] < upkeep:
            await ctx.send("Need $200 to enable auto-watering.")
            return
        player["money"] -= upkeep
        player["auto_watering"] = True
        save_data(data)
        await ctx.send("✅ Auto-watering enabled.")
        return
    player["auto_watering"] = False
    save_data(data)
    await ctx.send("🛑 Auto-watering disabled.")


@bot.command(name="buyfert")
async def buy_fertilizer_cmd(ctx: commands.Context, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Amount must be positive.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    cost = amount * 40
    if player["money"] < cost:
        await ctx.send(f"Need ${cost} for fertilizers.")
        return
    player["money"] -= cost
    player["fertilizers"] += amount
    save_data(data)
    await ctx.send(f"🧪 Bought {amount} fertilizers for ${cost}.")


@bot.command(name="buychem")
async def buy_chemical_cmd(ctx: commands.Context, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Amount must be positive.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    cost = amount * 70
    if player["money"] < cost:
        await ctx.send(f"Need ${cost} for chemicals.")
        return
    player["money"] -= cost
    player["chemicals"] += amount
    save_data(data)
    await ctx.send(f"⚗️ Bought {amount} chemicals for ${cost}.")


@bot.command(name="inventory")
async def inventory_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    inv = player.get("inventory", {}) or {}
    text = (
        f"{tr(player, 'inventory')}\n"
        f"💡 Lamps: {inv.get('lamps', 0)}\n"
        f"🧪 Fertilizers: {player.get('fertilizers', 0)}\n"
        f"⚗️ Chemicals: {player.get('chemicals', 0)}\n"
        f"🪙 Мёт: {player.get('meth', 0)}\n"
        f"🏅 Badge count: {len(player.get('badges', []))}\n"
        f"⚡ Energy: {player.get('energy', 0)}\n"
        f"🔔 Notify DM: {'On' if player.get('notify_dm', True) else 'Off'}"
    )
    save_data(data)
    await ctx.send(text)


@bot.command(name="meth")
async def meth_cmd(ctx: commands.Context, units: Optional[int] = None) -> None:
    """Convert ready joints to new currency: 1 мёт = 500 joints."""
    data = load_data()
    player = get_player(data, ctx.author)
    possible = player["joints"] // 500
    if possible <= 0:
        await ctx.send("Недостаточно косяков. Нужно минимум 500 косяков на 1 мёт.")
        return
    if units is None:
        units = possible
    if units <= 0 or units > possible:
        await ctx.send(f"Можно обменять от 1 до {possible} мёт.")
        return
    player["joints"] -= units * 500
    player["meth"] += units
    save_data(data)
    await ctx.send(f"🪙 Обмен выполнен: +{units} мёт за {units*500} косяков.")


@bot.command(name="recharge")
async def recharge_cmd(ctx: commands.Context, units: int) -> None:
    if units <= 0:
        await ctx.send("Укажи положительное количество.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    cost = units * 50
    if player["money"] < cost:
        await ctx.send(f"Недостаточно денег. Нужно ${cost}.")
        return
    player["money"] -= cost
    player["energy"] = min(200, int(player.get("energy", 100)) + units * 20)
    save_data(data)
    await ctx.send(f"⚡ Энергия пополнена до {player['energy']}.")


@bot.command(name="notify")
async def notify_cmd(ctx: commands.Context, mode: str) -> None:
    mode = mode.lower().strip()
    if mode not in {"on", "off"}:
        await ctx.send("Используй: !notify on|off")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    player["notify_dm"] = mode == "on"
    save_data(data)
    await ctx.send(f"🔔 DM-уведомления {'включены' if mode == 'on' else 'выключены'}.")


@bot.group(name="badges", invoke_without_command=True)
async def badges_group(ctx: commands.Context) -> None:
    await ctx.send("Используй: !badges shop | !badges buy <id> | !badges list")


@badges_group.command(name="shop")
async def badges_shop_cmd(ctx: commands.Context) -> None:
    lines = [f"{b['id']}. {b['name']} — {b['price_met']} мёт" for b in BADGE_SHOP]
    await ctx.send("🏅 Магазин бейджей (15 штук):\n" + "\n".join(lines))


@badges_group.command(name="buy")
async def badges_buy_cmd(ctx: commands.Context, badge_id: int) -> None:
    badge = next((b for b in BADGE_SHOP if b["id"] == badge_id), None)
    if not badge:
        await ctx.send("Бейдж не найден.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    if badge["name"] in player.get("badges", []):
        await ctx.send("У тебя уже есть этот бейдж.")
        return
    if player.get("meth", 0) < badge["price_met"]:
        await ctx.send("Недостаточно валюты мёт.")
        return
    player["meth"] -= badge["price_met"]
    player["badges"].append(badge["name"])
    save_data(data)
    await ctx.send(f"✅ Куплен бейдж: {badge['name']}")


@badges_group.command(name="list")
async def badges_list_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    items = player.get("badges", [])
    save_data(data)
    if not items:
        await ctx.send("У тебя пока нет бейджей.")
        return
    await ctx.send("🏅 Твои бейджи:\n" + "\n".join(items))


@bot.command(name="balance")
async def balance_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    mission_status = resolve_contraband(player)
    save_data(data)
    await ctx.send(embed=action_balance(player))
    if mission_status:
        await ctx.send(mission_status)


@bot.command(name="sell")
async def sell_cmd(ctx: commands.Context, item: str = "joints", amount: int = 1) -> None:
    if item.isdigit() and amount == 1:
        amount = int(item)
        item = "joints"
    if amount <= 0:
        await ctx.send("Количество должно быть > 0.")
        return
    item = item.lower().strip()
    data = load_data()
    player = get_player(data, ctx.author)
    level = max(1, int(player.get("level", 1)))
    price_mult = 1 + level * 0.03
    prices = {
        "joints": int((BASE_JOINT_PRICE + level * 2) * price_mult),
        "leaves_dry": int(8 * price_mult),
        "leaves_wet": int(5 * price_mult),
        "salt": int(16 * price_mult),
        "meth": int(240 * price_mult),
    }

    if item in prices:
        have = int(player.get(item, 0))
        if have < amount:
            await ctx.send("Недостаточно предметов.")
            return
        player[item] = have - amount
        total = prices[item] * amount
        player["money"] += total
        save_data(data)
        await ctx.send(f"💰 Продано {amount} x {item} за ${total}.")
        return

    # sell weapon
    weapons = player.setdefault("weapons", {})
    if item in WEAPON_STATS:
        have = int(weapons.get(item, 0))
        if have < amount:
            await ctx.send("Недостаточно такого оружия.")
            return
        weapons[item] = have - amount
        if weapons[item] <= 0:
            weapons.pop(item, None)
        total = int(WEAPON_STATS[item]["price"] * 0.65) * amount
        player["money"] += total
        save_data(data)
        await ctx.send(f"💰 Продано оружие {item} x{amount} за ${total}.")
        return
    await ctx.send("Нельзя продать этот предмет.")


@bot.command(name="blackmarket")
async def blackmarket_cmd(ctx: commands.Context) -> None:
    lines = [f"{k} — ${int(v['price'])}" for k, v in WEAPON_STATS.items()]
    rare = [f"{k} — ${v}" for k, v in BLACKMARKET_RARE.items()]
    await ctx.send("🕶️ Чёрный рынок:\n" + "\n".join(lines + ["-- редкое --"] + rare))


@bot.command(name="buyblack")
async def buyblack_cmd(ctx: commands.Context, item: str, amount: int = 1) -> None:
    if amount <= 0:
        await ctx.send("Количество должно быть > 0.")
        return
    item = item.lower().strip()
    data = load_data()
    player = get_player(data, ctx.author)
    player.setdefault("weapons", {})
    price = None
    target_dict = None
    if item in WEAPON_STATS:
        price = int(WEAPON_STATS[item]["price"])
        target_dict = player["weapons"]
    elif item in BLACKMARKET_RARE:
        price = int(BLACKMARKET_RARE[item])
        target_dict = player.setdefault("inventory", {})
    else:
        await ctx.send("Предмет не найден на чёрном рынке.")
        return
    total = price * amount
    if player["money"] < total:
        await ctx.send("Недостаточно денег.")
        return

    # police risk 15%
    if random.random() < 0.15:
        fine = min(player["money"], max(150, int(total * 0.25)))
        if player.get("influence", 0) > 50:
            fine = int(fine * 1.5)
        elif player.get("influence", 0) < -50:
            fine = int(fine * 0.7)
        player["money"] -= fine
        # confiscation chance
        if random.random() < 0.25:
            player["joints"] = max(0, player.get("joints", 0) - random.randint(1, 3))
        if random.random() < 0.25:
            player["meth"] = max(0, player.get("meth", 0) - 1)
        if random.random() < 0.25:
            player["salt"] = max(0, player.get("salt", 0) - random.randint(1, 2))
        clamp_player(player)
        save_data(data)
        log_event("BLACKMARKET_FAIL", str(ctx.author.id), f"fine={fine}")
        await ctx.send(f"🚓 Полиция накрыла сделку. Штраф: ${fine}.")
        return

    player["money"] -= total
    target_dict[item] = int(target_dict.get(item, 0)) + amount
    player["influence"] = min(100, int(player.get("influence", 0)) + 3)
    clamp_player(player)
    save_data(data)
    log_event("BLACKMARKET_BUY", str(ctx.author.id), f"{item} x{amount} total={total}")
    await ctx.send(f"✅ Куплено: {item} x{amount} за ${total}.")


@bot.command(name="loan")
async def loan_cmd(ctx: commands.Context, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Сумма займа должна быть > 0.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    loan = player.setdefault("loan", {"amount": 0, "updated_at": 0})
    if loan.get("amount", 0) > 0:
        await ctx.send("Сначала погаси текущий займ.")
        return
    cap = 2000 + player.get("level", 1) * 300
    if amount > cap:
        await ctx.send(f"Максимальный займ для тебя: ${cap}.")
        return
    player["money"] += amount
    loan["amount"] = int(amount * 1.15)
    loan["updated_at"] = now_ts()
    save_data(data)
    log_event("LOAN_TAKE", str(ctx.author.id), f"amount={amount} due={loan['amount']}")
    await ctx.send(f"🏦 Выдан займ ${amount}. К возврату: ${loan['amount']}.")


@bot.command(name="payloan")
async def payloan_cmd(ctx: commands.Context, amount: Optional[int] = None) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    loan = player.setdefault("loan", {"amount": 0, "updated_at": 0})
    due = int(loan.get("amount", 0))
    if due <= 0:
        await ctx.send("У тебя нет активного займа.")
        return
    pay = due if amount is None else max(0, int(amount))
    pay = min(pay, due)
    if player["money"] < pay:
        await ctx.send("Недостаточно денег для платежа.")
        return
    player["money"] -= pay
    loan["amount"] = due - pay
    loan["updated_at"] = now_ts()
    save_data(data)
    log_event("LOAN_PAY", str(ctx.author.id), f"pay={pay} remain={loan['amount']}")
    await ctx.send(f"💳 Оплачено ${pay}. Остаток по займу: ${loan['amount']}.")


@bot.command(name="use")
async def use_cmd(ctx: commands.Context, item: str) -> None:
    item = item.lower().strip()
    data = load_data()
    player = get_player(data, ctx.author)
    inv = player.setdefault("inventory", {})
    weapons = player.setdefault("weapons", {})
    if item == "lamp":
        if inv.get("lamps", 0) <= 0:
            await ctx.send("Нет ламп для использования.")
            return
        player["energy"] = min(200, int(player.get("energy", 100)) + 30)
        save_data(data)
        log_event("USE", str(ctx.author.id), "item=lamp")
        await ctx.send(f"💡 Лампа активирована. Энергия: {player['energy']}.")
        return
    if item == "fert":
        if player.get("fertilizers", 0) <= 0:
            await ctx.send("Нет удобрений.")
            return
        player["fertilizers"] -= 1
        player["upgrades"]["growth_level"] = player["upgrades"].get("growth_level", 0) + 1
        save_data(data)
        log_event("USE", str(ctx.author.id), "item=fert")
        await ctx.send("🧪 Удобрение использовано: рост ускорен.")
        return
    if item == "chem":
        if player.get("chemicals", 0) <= 0:
            await ctx.send("Нет химикатов.")
            return
        player["chemicals"] -= 1
        player["smoke_until"] = now_ts() + 1200
        save_data(data)
        log_event("USE", str(ctx.author.id), "item=chem")
        await ctx.send("⚗️ Химикат использован: временный буст активен.")
        return
    if item == "salt":
        if player.get("salt", 0) <= 0:
            await ctx.send("Нет соли.")
            return
        player["salt"] -= 1
        player["money"] += 50
        save_data(data)
        log_event("USE", str(ctx.author.id), "item=salt")
        await ctx.send("🤧 Нюхнул соли... +$50.")
        return
    if item == "meth":
        if player.get("meth", 0) <= 0:
            await ctx.send("Нет мёта.")
            return
        player["meth"] -= 1
        player["meth_until"] = now_ts() + 1800
        player["influence"] = min(100, int(player.get("influence", 0)) + 5)
        if random.random() < 0.10:
            fine = min(player["money"], 120)
            player["money"] -= fine
        save_data(data)
        log_event("USE", str(ctx.author.id), "item=meth")
        await ctx.send("🧪 Meth использован: +30% к шансу рейда на 30 минут.")
        return
    if item in WEAPON_STATS:
        if weapons.get(item, 0) <= 0:
            await ctx.send("Нет такого оружия.")
            return
        player["active_weapon"] = item
        player["weapon_until"] = now_ts() + 1800
        weapons[item] -= 1
        if weapons[item] <= 0:
            weapons.pop(item, None)
        save_data(data)
        update_player_http(str(ctx.author.id), {"active_weapon": item, "weapon_until": player["weapon_until"], "weapons": weapons})
        log_event("USE", str(ctx.author.id), f"weapon={item}")
        await ctx.send(f"🔫 Оружие {item} активировано на 30 минут.")
        return
    await ctx.send("Этот предмет нельзя использовать.")


@bot.command(name="weapon")
async def weapon_cmd(ctx: commands.Context, name: str) -> None:
    name = name.lower().strip()
    data = load_data()
    player = get_player(data, ctx.author)
    weapons = player.setdefault("weapons", {})
    if weapons.get(name, 0) <= 0:
        await ctx.send("У тебя нет такого оружия.")
        return
    player["active_weapon"] = name
    player["weapon_until"] = now_ts() + 1800
    save_data(data)
    update_player_http(str(ctx.author.id), {"active_weapon": name, "weapon_until": player["weapon_until"]})
    await ctx.send(f"🔫 Активное оружие: {name}")


@bot.command(name="duel")
async def duel_cmd(ctx: commands.Context, target: discord.Member, amount: int) -> None:
    if target.bot or target.id == ctx.author.id:
        await ctx.send("Invalid duel target.")
        return
    if amount <= 0:
        await ctx.send("Amount must be positive.")
        return

    data = load_data()
    p1 = get_player(data, ctx.author)
    p2 = get_player(data, target)
    if p1["money"] < amount or p2["money"] < amount:
        await ctx.send("Оба игрока должны иметь нужную сумму.")
        return
    p1["money"] -= amount
    p2["money"] -= amount
    duel_id = str(data["meta"]["next_duel_id"])
    data["meta"]["next_duel_id"] += 1
    data["meta"]["duels"][duel_id] = {
        "id": duel_id,
        "fighter1": str(ctx.author.id),
        "fighter2": str(target.id),
        "pot": amount * 2,
        "ends_at": now_ts() + 25,
        "bets": [],
        "resolved": False,
    }
    save_data(data)
    await ctx.send(
        f"⚔️ Дуэль #{duel_id} началась: {ctx.author.mention} vs {target.mention}.\n"
        f"Банк: ${amount*2}. Ставки зрителей: `!betduel {duel_id} @игрок сумма` в течение 25с.\n"
        f"Завершение: `!resolveduel {duel_id}`"
    )


@bot.command(name="betduel")
async def bet_duel_cmd(ctx: commands.Context, duel_id: str, fighter: discord.Member, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Ставка должна быть положительной.")
        return
    data = load_data()
    bettor = get_player(data, ctx.author)
    duel = data["meta"]["duels"].get(duel_id)
    if not duel or duel.get("resolved"):
        await ctx.send("Дуэль не найдена.")
        return
    if now_ts() > int(duel["ends_at"]):
        await ctx.send("Окно ставок закрыто.")
        return
    if str(fighter.id) not in {duel["fighter1"], duel["fighter2"]}:
        await ctx.send("Можно ставить только на участника дуэли.")
        return
    if bettor["money"] < amount:
        await ctx.send("Недостаточно денег для ставки.")
        return
    bettor["money"] -= amount
    duel["bets"].append(
        {"user_id": str(ctx.author.id), "fighter_id": str(fighter.id), "amount": amount}
    )
    save_data(data)
    await ctx.send(f"💸 Ставка принята: ${amount} на {fighter.mention} в дуэли #{duel_id}.")


@bot.command(name="resolveduel")
async def resolve_duel_cmd(ctx: commands.Context, duel_id: str) -> None:
    data = load_data()
    duel = data["meta"]["duels"].get(duel_id)
    if not duel or duel.get("resolved"):
        await ctx.send("Дуэль не найдена или уже завершена.")
        return
    if now_ts() < int(duel["ends_at"]):
        await ctx.send("Дуэль ещё идёт.")
        return
    if str(ctx.author.id) not in {duel["fighter1"], duel["fighter2"]}:
        await ctx.send("Только участник дуэли может завершить её.")
        return

    winner_id = duel["fighter1"] if random.random() < 0.5 else duel["fighter2"]
    winner = data["players"].get(winner_id)
    if winner:
        winner["money"] += int(duel["pot"])
        winner["reputation"]["street"] = winner["reputation"].get("street", 0) + 2
        add_xp(winner, 14)

    total_bets = sum(int(b["amount"]) for b in duel["bets"])
    winners_pool = sum(int(b["amount"]) for b in duel["bets"] if b["fighter_id"] == winner_id)
    for bet in duel["bets"]:
        if bet["fighter_id"] != winner_id:
            continue
        p = data["players"].get(bet["user_id"])
        if not p:
            continue
        share = bet["amount"] / max(1, winners_pool)
        payout = int(share * total_bets)
        p["money"] += payout

    duel["resolved"] = True
    save_data(data)
    await ctx.send(f"🏁 Дуэль #{duel_id} завершена. Победитель: <@{winner_id}>.")


@bot.command(name="casino")
async def casino_cmd(ctx: commands.Context, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Amount must be positive.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    if player["money"] < amount:
        await ctx.send("Not enough money.")
        return

    roll = random.random()
    if roll < 0.45:
        player["money"] -= amount
        result = f"🎰 Lost ${amount}."
    elif roll < 0.9:
        player["money"] += amount
        result = f"🎉 Won ${amount}!"
    else:
        jackpot = amount * 3
        player["money"] += jackpot
        result = f"💎 JACKPOT! Won ${jackpot}!"

    add_xp(player, 10)
    player["reputation"]["street"] = player["reputation"].get("street", 0) + 1
    save_data(data)
    await ctx.send(result)


@bot.command(name="daily")
async def daily_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    left = cooldown_left(player, "daily", DAILY_COOLDOWN)
    if left > 0:
        await ctx.send(f"⏳ Daily cooldown: {left}s")
        return

    reward = random.randint(100, 220)
    player["money"] += reward
    player["cooldowns"]["daily"] = now_ts()
    add_xp(player, 10)
    save_data(data)
    await ctx.send(f"📦 Daily reward: ${reward}")


@bot.command(name="fightpolice")
async def fight_police_cmd(ctx: commands.Context) -> None:
    data = load_data()
    raid = data["meta"].get("active_raid")
    if not raid:
        await ctx.send("No active police raid right now.")
        return
    uid = str(ctx.author.id)
    if uid in raid["fighters"]:
        await ctx.send("You already joined the raid defense.")
        return
    raid["fighters"].append(uid)
    player = get_player(data, ctx.author)
    player["reputation"]["street"] = player["reputation"].get("street", 0) + 2
    save_data(data)
    await ctx.send("🛡️ You joined the raid defense!")


@bot.command(name="raid")
async def raid_cmd(ctx: commands.Context, target: discord.Member) -> None:
    if target.bot or target.id == ctx.author.id:
        await ctx.send("Неверная цель рейда.")
        return
    data = load_data()
    attacker = get_player(data, ctx.author)
    victim = get_player(data, target)

    # anti-abuse limits
    raid = attacker.setdefault("raid", {"day": 0, "count": 0, "last_ts": 0})
    today = day_bucket()
    if raid.get("day") != today:
        raid["day"] = today
        raid["count"] = 0
        attacker["raid_targets"] = {}
    if int(raid.get("count", 0)) >= 5:
        await ctx.send("Лимит рейдов на сегодня (5) исчерпан.")
        save_data(data)
        return
    if now_ts() - int(raid.get("last_ts", 0)) < 600:
        await ctx.send("Кулдаун рейда: 10 минут.")
        save_data(data)
        return

    if abs(int(attacker.get("level", 1)) - int(victim.get("level", 1))) > 5:
        await ctx.send("Разница уровней слишком большая (макс 5).")
        save_data(data)
        return
    if int(victim.get("money", 0)) < 100:
        await ctx.send("Цель слишком бедная для рейда.")
        save_data(data)
        return
    if int(attacker.get("money", 0)) < 300 or int(victim.get("money", 0)) < 300:
        await ctx.send("Анти-твинк: при балансе < 300 рейды запрещены.")
        save_data(data)
        return
    targets = attacker.setdefault("raid_targets", {})
    tgt_key = str(target.id)
    if int(targets.get(tgt_key, 0)) >= 2:
        await ctx.send("Анти-фарм: нельзя рейдить одного игрока более 2 раз в день.")
        save_data(data)
        return

    chance = 0.50
    weapon_bonus = active_weapon_bonus(attacker)
    if now_ts() < int(victim.get("shield_until", 0)):
        weapon_bonus *= 0.5
        chance -= 0.18
    chance += weapon_bonus
    chance += active_meth_bonus(attacker)
    if int(victim.get("money", 0)) < 200:
        chance -= 0.15
    chance = max(0.1, min(0.9, chance))

    raid["count"] = int(raid.get("count", 0)) + 1
    raid["last_ts"] = now_ts()
    targets[tgt_key] = int(targets.get(tgt_key, 0)) + 1

    if random.random() < chance:
        stolen = max(20, int(victim["money"] * random.uniform(0.07, 0.18)))
        stolen = min(stolen, victim["money"])
        victim["money"] -= stolen
        attacker["money"] += stolen
        msg = f"⚔️ Успешный рейд! Добыча: ${stolen}."
    else:
        penalty = min(attacker["money"], random.randint(20, 80))
        attacker["money"] -= penalty
        msg = f"🚨 Рейд провален. Потеряно ${penalty}."

    attacker["influence"] = min(100, int(attacker.get("influence", 0)) + 2)
    clamp_player(attacker)
    clamp_player(victim)
    save_data(data)
    await ctx.send(msg)


@bot.group(name="farm", invoke_without_command=True)
async def farm_group(ctx: commands.Context) -> None:
    await ctx.send("Use: !farm create <name> | !farm join <name> | !farm info [name]")


@farm_group.command(name="create")
async def farm_create(ctx: commands.Context, *, name: str) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    lname = name.lower().strip()
    if not lname:
        await ctx.send("Farm name cannot be empty.")
        return
    if lname in data["farms"]:
        await ctx.send("Farm already exists.")
        return
    data["farms"][lname] = {
        "name": name,
        "owner": str(ctx.author.id),
        "members": [str(ctx.author.id)],
        "avatar_url": ctx.message.attachments[0].url if ctx.message.attachments else None,
    }
    player["farm"] = lname
    save_data(data)
    avatar_info = "\nАватар: прикреплён." if ctx.message.attachments else "\nАватар: не задан."
    await ctx.send(f"🏡 Farm **{name}** created.{avatar_info}")


@farm_group.command(name="join")
async def farm_join(ctx: commands.Context, *, name: str) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    lname = name.lower().strip()
    farm = data["farms"].get(lname)
    if not farm:
        await ctx.send("Farm not found.")
        return
    uid = str(ctx.author.id)
    if uid not in farm["members"]:
        farm["members"].append(uid)
    player["farm"] = lname
    save_data(data)
    await ctx.send(f"🤝 Joined farm **{farm['name']}**.")


@farm_group.command(name="info")
async def farm_info(ctx: commands.Context, *, name: Optional[str] = None) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    key = name.lower().strip() if name else player.get("farm")
    farm = data["farms"].get(key) if key else None
    if not farm:
        await ctx.send("Farm not found.")
        return
    member_names = []
    for uid in farm["members"]:
        p = data["players"].get(uid)
        member_names.append(p["username"] if p else uid)
    await ctx.send(
        f"🏡 **{farm['name']}**\nOwner: <@{farm['owner']}>\nMembers: {', '.join(member_names)}\n"
        f"Avatar: {farm.get('avatar_url') or 'нет'}"
    )


@bot.command(name="farmrank")
async def farm_rank_cmd(ctx: commands.Context) -> None:
    data = load_data()
    ranking = []
    for key, farm in data["farms"].items():
        member_money = 0
        for uid in farm.get("members", []):
            p = data["players"].get(uid)
            if p:
                member_money += int(p.get("money", 0))
        score = member_money + len(farm.get("members", [])) * 500
        ranking.append((farm["name"], score, len(farm.get("members", []))))
    ranking.sort(key=lambda x: x[1], reverse=True)
    if not ranking:
        await ctx.send("Нет данных по фермам.")
        return
    lines = [f"{i+1}. {name} | score: {score} | members: {members}" for i, (name, score, members) in enumerate(ranking[:10])]
    await ctx.send("🏆 Рейтинг ферм:\n" + "\n".join(lines))


@bot.command(name="upgrade")
async def upgrade_cmd(ctx: commands.Context) -> None:
    await ctx.send("🛒 Upgrade Shop", view=UpgradeShopView())


@bot.group(name="cartel", invoke_without_command=True)
async def cartel_group(ctx: commands.Context) -> None:
    await ctx.send("Используй: !cartel create <name> | !cartel join <name> | !cartel info | !cartel deposit <sum> | !cartel withdraw <sum>")


@cartel_group.command(name="create")
async def cartel_create(ctx: commands.Context, *, name: str) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    key = name.lower().strip()
    if key in data["cartels"]:
        await ctx.send("Картель уже существует.")
        return
    data["cartels"][key] = {"name": name, "owner": str(ctx.author.id), "members": [str(ctx.author.id)], "bank": 0, "storage": {}}
    player["cartel"] = key
    save_data(data)
    await ctx.send(f"🕶️ Картель **{name}** создан.")


@cartel_group.command(name="join")
async def cartel_join(ctx: commands.Context, *, name: str) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    key = name.lower().strip()
    cartel = data["cartels"].get(key)
    if not cartel:
        await ctx.send("Картель не найден.")
        return
    uid = str(ctx.author.id)
    if uid not in cartel["members"]:
        cartel["members"].append(uid)
    player["cartel"] = key
    save_data(data)
    await ctx.send(f"🤝 Вы вступили в картель **{cartel['name']}**.")


@cartel_group.command(name="info")
async def cartel_info(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    key = player.get("cartel")
    cartel = data["cartels"].get(key) if key else None
    if not cartel:
        await ctx.send("Вы не состоите в картеле.")
        return
    await ctx.send(
        f"🕶️ **{cartel['name']}**\nБанк: ${cartel['bank']}\nУчастников: {len(cartel['members'])}\nВладелец: <@{cartel['owner']}>"
    )


@cartel_group.command(name="deposit")
async def cartel_deposit(ctx: commands.Context, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Сумма должна быть положительной.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    key = player.get("cartel")
    cartel = data["cartels"].get(key) if key else None
    if not cartel:
        await ctx.send("Сначала вступите в картель.")
        return
    if player["money"] < amount:
        await ctx.send("Недостаточно средств.")
        return
    player["money"] -= amount
    cartel["bank"] += amount
    save_data(data)
    await ctx.send(f"💰 В банк картеля внесено ${amount}.")


@cartel_group.command(name="withdraw")
async def cartel_withdraw(ctx: commands.Context, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Сумма должна быть положительной.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    key = player.get("cartel")
    cartel = data["cartels"].get(key) if key else None
    if not cartel:
        await ctx.send("Сначала вступите в картель.")
        return
    if str(ctx.author.id) != cartel["owner"]:
        await ctx.send("Снимать может только владелец картеля.")
        return
    if cartel["bank"] < amount:
        await ctx.send("В банке картеля недостаточно средств.")
        return
    cartel["bank"] -= amount
    player["money"] += amount
    save_data(data)
    await ctx.send(f"🏦 Из банка картеля снято ${amount}.")


@bot.group(name="clan", invoke_without_command=True)
async def clan_group(ctx: commands.Context) -> None:
    await ctx.send("Используй: !clan store <item> <amount> | !clan take <item> <amount>")


@clan_group.command(name="store")
async def clan_store_cmd(ctx: commands.Context, item: str, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Количество должно быть > 0.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    cartel_key = player.get("cartel")
    cartel = data["cartels"].get(cartel_key) if cartel_key else None
    if not cartel:
        await ctx.send("Ты не в клане/картеле.")
        return
    cartel.setdefault("storage", {})
    have = int(player.get(item, 0))
    if have < amount:
        await ctx.send("Недостаточно предметов.")
        return
    player[item] = have - amount
    cartel["storage"][item] = int(cartel["storage"].get(item, 0)) + amount
    save_data(data)
    await ctx.send(f"📦 В склад клана добавлено: {item} x{amount}.")


@clan_group.command(name="take")
async def clan_take_cmd(ctx: commands.Context, item: str, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Количество должно быть > 0.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    cartel_key = player.get("cartel")
    cartel = data["cartels"].get(cartel_key) if cartel_key else None
    if not cartel:
        await ctx.send("Ты не в клане/картеле.")
        return
    cartel.setdefault("storage", {})
    have = int(cartel["storage"].get(item, 0))
    if have < amount:
        await ctx.send("На складе недостаточно предметов.")
        return
    cartel["storage"][item] = have - amount
    player[item] = int(player.get(item, 0)) + amount
    save_data(data)
    await ctx.send(f"📦 Со склада клана взято: {item} x{amount}.")


@bot.command(name="reputation")
async def reputation_cmd(ctx: commands.Context) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    rep = player.get("reputation", {})
    save_data(data)
    await ctx.send(f"📊 Репутация\nStreet: {rep.get('street', 0)}\nPolice: {rep.get('police', 0)}")


@bot.group(name="casinotour", invoke_without_command=True)
async def casino_tour_group(ctx: commands.Context) -> None:
    await ctx.send("Используй: !casinotour create <buyin> | !casinotour join <id> | !casinotour start <id>")


@casino_tour_group.command(name="create")
async def casino_tour_create(ctx: commands.Context, buyin: int) -> None:
    if buyin <= 0:
        await ctx.send("Buy-in должен быть > 0.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    if player["money"] < buyin:
        await ctx.send("Недостаточно денег.")
        return
    player["money"] -= buyin
    tid = str(data["meta"]["next_tournament_id"])
    data["meta"]["next_tournament_id"] += 1
    data["tournaments"][tid] = {
        "id": tid,
        "owner": str(ctx.author.id),
        "buyin": buyin,
        "participants": [str(ctx.author.id)],
        "started": False,
    }
    save_data(data)
    await ctx.send(f"🎰 Турнир #{tid} создан. Взнос ${buyin}. Вход: !casinotour join {tid}")


@casino_tour_group.command(name="join")
async def casino_tour_join(ctx: commands.Context, tid: str) -> None:
    data = load_data()
    player = get_player(data, ctx.author)
    tour = data["tournaments"].get(tid)
    if not tour or tour.get("started"):
        await ctx.send("Турнир не найден.")
        return
    if str(ctx.author.id) in tour["participants"]:
        await ctx.send("Вы уже участвуете.")
        return
    buyin = int(tour["buyin"])
    if player["money"] < buyin:
        await ctx.send("Недостаточно денег.")
        return
    player["money"] -= buyin
    tour["participants"].append(str(ctx.author.id))
    save_data(data)
    await ctx.send(f"✅ Вы вошли в турнир #{tid}.")


@casino_tour_group.command(name="start")
async def casino_tour_start(ctx: commands.Context, tid: str) -> None:
    data = load_data()
    tour = data["tournaments"].get(tid)
    if not tour or tour.get("started"):
        await ctx.send("Турнир не найден.")
        return
    if str(ctx.author.id) != tour["owner"]:
        await ctx.send("Запускать может только создатель.")
        return
    players = tour["participants"]
    if len(players) < 2:
        await ctx.send("Нужно минимум 2 участника.")
        return
    winner_id = random.choice(players)
    pot = int(tour["buyin"]) * len(players)
    winner = data["players"].get(winner_id)
    if winner:
        winner["money"] += int(pot * 0.9)
        winner["reputation"]["street"] = winner["reputation"].get("street", 0) + 3
    tour["started"] = True
    save_data(data)
    await ctx.send(f"🏆 Турнир #{tid} завершён. Победитель: <@{winner_id}>. Приз: ${int(pot*0.9)}")


@bot.command(name="exchange")
async def exchange_cmd(ctx: commands.Context, joints: int) -> None:
    if joints <= 0:
        await ctx.send("Amount must be positive.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    if player["joints"] < joints:
        await ctx.send("Not enough joints.")
        return
    salt = joints * 2
    player["joints"] -= joints
    player["salt"] += salt
    add_xp(player, 8)
    save_data(data)
    await ctx.send(f"🧂 Exchanged {joints} joints for {salt} salt.")


@bot.command(name="contraband")
async def contraband_cmd(ctx: commands.Context, country: str) -> None:
    data = load_data()
    player = get_player(data, ctx.author)

    status = resolve_contraband(player)
    if status and "still running" in status:
        save_data(data)
        await ctx.send(status)
        return

    key = country.lower()
    if key not in CONTRABAND_COUNTRIES:
        await ctx.send(
            "Valid countries: China, Russia, Ukraine, Canada, USA, Brazil, Venezuela"
        )
        return

    cfg = CONTRABAND_COUNTRIES[key]
    reward = random.randint(cfg["min"], cfg["max"])
    duration = cfg["duration"]
    player["contraband"] = {
        "country": country.title(),
        "ends_at": now_ts() + duration,
        "reward": reward,
    }
    save_data(data)
    await ctx.send(
        f"🧳 Contraband to **{country.title()}** started. "
        f"Return in **{duration}s** for payout."
    )


@bot.command(name="leaderboard")
async def leaderboard_cmd(ctx: commands.Context) -> None:
    data = load_data()
    players = list(data["players"].values())
    if not players:
        await ctx.send("No leaderboard data yet.")
        return
    top_money = sorted(players, key=lambda p: p.get("money", 0), reverse=True)[:10]
    top_level = sorted(players, key=lambda p: p.get("level", 1), reverse=True)[:10]

    embed = discord.Embed(title="🏆 Leaderboards", color=discord.Color.gold())
    embed.add_field(
        name="Top Money",
        value="\n".join(
            f"{i+1}. {p['username']} - ${p['money']}" for i, p in enumerate(top_money)
        ),
        inline=False,
    )
    embed.add_field(
        name="Top Level",
        value="\n".join(
            f"{i+1}. {p['username']} - Lv {p['level']}" for i, p in enumerate(top_level)
        ),
        inline=False,
    )
    await ctx.send(embed=embed, view=LeaderboardView())


class LeaderboardView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    @discord.ui.button(label="Подробнее", style=discord.ButtonStyle.secondary)
    async def details(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        data = load_data()
        player = get_player(data, interaction.user)
        short_inv = (
            f"lamp:{player.get('inventory', {}).get('lamps', 0)}, "
            f"fert:{player.get('fertilizers', 0)}, chem:{player.get('chemicals', 0)}, "
            f"salt:{player.get('salt', 0)}, meth:{player.get('meth', 0)}"
        )
        weapons = player.get("weapons", {})
        weapon_text = ", ".join([f"{k}:{v}" for k, v in weapons.items()]) if weapons else "нет"
        cartel = player.get("cartel") or "нет"
        debt = player.get("loan", {}).get("amount", 0)
        text = (
            f"💰 Деньги: ${player.get('money', 0)}\n"
            f"⭐ Уровень: {player.get('level', 1)}\n"
            f"💳 Долг: ${debt}\n"
            f"🕶️ Клан/картель: {cartel}\n"
            f"🎒 Инвентарь: {short_inv}\n"
            f"🔫 Оружие: {weapon_text}\n"
            f"📈 Influence: {player.get('influence', 0)}"
        )
        save_data(data)
        await safe_interaction_reply(interaction, content=text, ephemeral=True)


@bot.command(name="trade")
async def trade_cmd(ctx: commands.Context, target: discord.Member, joints: int) -> None:
    if target.bot or target.id == ctx.author.id:
        await ctx.send("Invalid trade target.")
        return
    if joints <= 0:
        await ctx.send("Amount must be positive.")
        return

    data = load_data()
    sender = get_player(data, ctx.author)
    receiver = get_player(data, target)
    if sender["joints"] < joints:
        await ctx.send("You do not have enough joints.")
        return

    sender["joints"] -= joints
    receiver["joints"] += joints
    add_xp(sender, 5)
    save_data(data)
    await ctx.send(f"🤝 Traded {joints} joints to {target.mention}.")


@bot.command(name="transfer")
async def transfer_cmd(ctx: commands.Context, target: discord.Member, amount: int) -> None:
    if target.bot or target.id == ctx.author.id:
        await ctx.send("Неверная цель перевода.")
        return
    if amount <= 0 or amount > 5000:
        await ctx.send("За один раз можно перевести от 1 до 5000.")
        return
    data = load_data()
    sender = get_player(data, ctx.author)
    receiver = get_player(data, target)
    tr_state = sender.setdefault("transfer", {"day": day_bucket(), "sent": 0})
    if int(tr_state.get("day", day_bucket())) != day_bucket():
        tr_state["day"] = day_bucket()
        tr_state["sent"] = 0
    if int(tr_state.get("sent", 0)) + amount > 20000:
        await ctx.send("Дневной лимит переводов: 20000.")
        save_data(data)
        return
    if sender.get("money", 0) < amount:
        await ctx.send("Недостаточно денег.")
        return
    sender["money"] -= amount
    receiver["money"] += amount
    tr_state["sent"] = int(tr_state.get("sent", 0)) + amount
    clamp_player(sender)
    clamp_player(receiver)
    save_data(data)
    log_event("TRANSFER", str(ctx.author.id), f"to={target.id} amount={amount}")
    await ctx.send(f"💸 Перевод ${amount} отправлен {target.mention}.")


@bot.group(name="auction", invoke_without_command=True)
async def auction_group(ctx: commands.Context) -> None:
    await ctx.send("Use: !auction create <joints> <min_bid> | !auction list | !auction bid <id> <amount> | !auction claim <id>")


@auction_group.command(name="create")
async def auction_create(ctx: commands.Context, joints: int, min_bid: int) -> None:
    if joints <= 0 or min_bid <= 0:
        await ctx.send("Values must be positive.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    if player["joints"] < joints:
        await ctx.send("Not enough joints.")
        return

    auction_id = str(data["meta"]["next_auction_id"])
    data["meta"]["next_auction_id"] += 1
    player["joints"] -= joints
    data["auctions"][auction_id] = {
        "id": auction_id,
        "seller": str(ctx.author.id),
        "joints": joints,
        "min_bid": min_bid,
        "highest_bid": min_bid - 1,
        "highest_bidder": None,
        "ends_at": now_ts() + 90,
        "claimed": False,
    }
    save_data(data)
    await ctx.send(f"📢 Auction #{auction_id} created: {joints} joints, minimum ${min_bid}, ends in 90s.")


@auction_group.command(name="list")
async def auction_list(ctx: commands.Context) -> None:
    data = load_data()
    auctions = []
    for item in data["auctions"].values():
        if now_ts() <= int(item["ends_at"]) and not item.get("claimed"):
            left = int(item["ends_at"]) - now_ts()
            top = item["highest_bid"] if item["highest_bidder"] else item["min_bid"]
            auctions.append(f"#{item['id']} | {item['joints']} joints | top ${top} | {left}s left")
    if not auctions:
        await ctx.send("No active auctions.")
        return
    await ctx.send("🧾 Active auctions:\n" + "\n".join(auctions[:20]))


@auction_group.command(name="bid")
async def auction_bid(ctx: commands.Context, auction_id: str, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Amount must be positive.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    auction = data["auctions"].get(auction_id)
    if not auction:
        await ctx.send("Auction not found.")
        return
    if now_ts() > int(auction["ends_at"]):
        await ctx.send("Auction already ended. Use !auction claim.")
        return
    if auction["seller"] == str(ctx.author.id):
        await ctx.send("You cannot bid on your own auction.")
        return
    min_required = max(int(auction["min_bid"]), int(auction["highest_bid"]) + 1)
    if amount < min_required:
        await ctx.send(f"Bid must be at least ${min_required}.")
        return
    if player["money"] < amount:
        await ctx.send("Not enough money.")
        return
    # Refund previous bidder instantly.
    prev_bidder = auction.get("highest_bidder")
    prev_amount = int(auction.get("highest_bid", 0))
    if prev_bidder:
        prev_player = data["players"].get(prev_bidder)
        if prev_player:
            prev_player["money"] += prev_amount
    player["money"] -= amount
    auction["highest_bid"] = amount
    auction["highest_bidder"] = str(ctx.author.id)
    save_data(data)
    await ctx.send(f"💸 Bid placed on auction #{auction_id}: ${amount}.")


@auction_group.command(name="claim")
async def auction_claim(ctx: commands.Context, auction_id: str) -> None:
    data = load_data()
    auction = data["auctions"].get(auction_id)
    if not auction:
        await ctx.send("Auction not found.")
        return
    if now_ts() <= int(auction["ends_at"]):
        await ctx.send("Auction not finished yet.")
        return
    if auction.get("claimed"):
        await ctx.send("Auction already claimed.")
        return

    seller_id = auction["seller"]
    bidder_id = auction.get("highest_bidder")
    seller = data["players"].get(seller_id)
    if not seller:
        await ctx.send("Seller data unavailable.")
        return

    caller = str(ctx.author.id)
    if caller not in {seller_id, bidder_id}:
        await ctx.send("Only seller or winner can claim this auction.")
        return

    if bidder_id:
        bidder = data["players"].get(bidder_id)
        if bidder:
            bidder["joints"] += int(auction["joints"])
        seller["money"] += int(auction["highest_bid"])
        msg = f"✅ Auction #{auction_id} complete. Winner: <@{bidder_id}>."
    else:
        seller["joints"] += int(auction["joints"])
        msg = f"ℹ️ Auction #{auction_id} ended without bids. Goods returned to seller."

    auction["claimed"] = True
    save_data(data)
    log_event("RAID", str(ctx.author.id), f"target={target.id} chance={chance:.2f} result={msg}")
    await ctx.send(msg)


@bot.group(name="vipauction", invoke_without_command=True)
async def vip_auction_group(ctx: commands.Context) -> None:
    await ctx.send("Используй: !vipauction create <joints> <min_bid> | !vipauction list")


@vip_auction_group.command(name="create")
async def vip_auction_create(ctx: commands.Context, joints: int, min_bid: int) -> None:
    if joints <= 0 or min_bid <= 0:
        await ctx.send("Значения должны быть > 0.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    if player.get("meth", 0) < 1:
        await ctx.send("Для VIP-аукциона нужен 1 мёт.")
        return
    if player["joints"] < joints:
        await ctx.send("Недостаточно косяков.")
        return
    player["meth"] -= 1
    player["joints"] -= joints
    auction_id = str(data["meta"]["next_auction_id"])
    data["meta"]["next_auction_id"] += 1
    data["auctions"][auction_id] = {
        "id": auction_id,
        "seller": str(ctx.author.id),
        "joints": joints,
        "min_bid": min_bid,
        "highest_bid": min_bid - 1,
        "highest_bidder": None,
        "ends_at": now_ts() + 180,
        "claimed": False,
        "vip": True,
    }
    save_data(data)
    await ctx.send(f"💎 VIP-аукцион #{auction_id} создан (списано 1 мёт).")


@vip_auction_group.command(name="list")
async def vip_auction_list(ctx: commands.Context) -> None:
    data = load_data()
    lines = []
    for item in data["auctions"].values():
        if not item.get("vip"):
            continue
        if now_ts() > int(item["ends_at"]) or item.get("claimed"):
            continue
        left = int(item["ends_at"]) - now_ts()
        top = item["highest_bid"] if item["highest_bidder"] else item["min_bid"]
        lines.append(f"#{item['id']} | {item['joints']} joints | top ${top} | {left}s")
    if not lines:
        await ctx.send("Нет активных VIP-аукционов.")
        return
    await ctx.send("💎 VIP-аукционы:\n" + "\n".join(lines))


@bot.command(name="help")
async def help_cmd(ctx: commands.Context) -> None:
    await ctx.send(
        "Команды: !menu !about !plant !harvest !dry !roll !smoke !balance "
        "!sell <amount> !duel @user <amount> !casino <amount> !daily !fightpolice "
        "!farm create/join/info !farmrank !upgrade !exchange <joints> !contraband <country> "
        "!leaderboard !trade @user <joints> !water !care !autowater on/off "
        "!buyfert <n> !buychem <n> !inventory !meth [units] !recharge <units> !notify on/off !badges ... !blackmarket !buyblack !loan !payloan !raid @user !transfer @user <sum> "
        "!auction create/list/bid/claim !vipauction ... "
        "!cartel ... !reputation !casinotour ... !betduel !resolveduel"
    )


# ================================================================
# Background loops
# ================================================================
@tasks.loop(seconds=AUTO_INCOME_INTERVAL)
async def auto_income_loop() -> None:
    data = load_data()
    for uid, player in data["players"].items():
        # passive influence decay
        if player.get("influence", 0) > 0:
            player["influence"] = int(player.get("influence", 0)) - 1
        elif player.get("influence", 0) < 0:
            player["influence"] = int(player.get("influence", 0)) + 1

        # workers salary once per day
        if int(player.get("workers", 0)) > 0 and int(player.get("last_salary_day", day_bucket())) != day_bucket():
            salary = int(player.get("workers", 0)) * 500
            if int(player.get("money", 0)) >= salary:
                player["money"] -= salary
                player["workers_active"] = True
            else:
                player["workers_active"] = False
            player["last_salary_day"] = day_bucket()

        base = 15 + player["level"] * 2 + player["upgrades"]["farm_level"] * 3
        if int(player.get("workers", 0)) > 0 and player.get("workers_active", True):
            base += int(player.get("workers", 0)) * 5
        if now_ts() < int(player.get("smoke_until", 0)):
            base += 10
        lamps = int(player.get("inventory", {}).get("lamps", 0))
        if lamps > 0:
            player["energy"] = max(0, int(player.get("energy", 100)) - lamps)
        if player.get("auto_watering") and player["plants"]:
            for plant in player["plants"]:
                plant["last_watered"] = now_ts()
            base -= 8

        # Idea #40: DM notifications when contraband resolves automatically.
        mission_status = resolve_contraband(player)
        if mission_status and ("✅" in mission_status or "🚓" in mission_status):
            if player.get("notify_dm", True):
                user = bot.get_user(int(uid))
                if user:
                    try:
                        await user.send(f"Контрабанда: {mission_status}")
                    except Exception:
                        pass
        player["money"] += base
        clamp_player(player)
    save_data(data)


@tasks.loop(seconds=TAX_INTERVAL)
async def tax_loop() -> None:
    data = load_data()
    total_tax = 0
    for player in data["players"].values():
        money = int(player.get("money", 0))
        if money <= 0:
            continue
        tax = int(money * 0.2)
        if tax > 0:
            player["money"] -= tax
            player["influence"] = max(-100, int(player.get("influence", 0)) - 2)
            clamp_player(player)
            total_tax += tax
    save_data(data)

    if total_tax <= 0:
        return
    for guild in bot.guilds:
        channel = guild.system_channel or next(
            (c for c in guild.text_channels if c.permissions_for(guild.me).send_messages),
            None,
        )
        if channel:
            await channel.send(f"💸 Gang boss collected taxes: **${total_tax}** (20%).")


@tasks.loop(seconds=RAID_INTERVAL)
async def police_loop() -> None:
    data = load_data()
    raid = data["meta"].get("active_raid")

    if raid and now_ts() >= int(raid["ends_at"]):
        fighters = raid.get("fighters", [])
        defended = len(fighters) >= 2

        if defended:
            for uid in fighters:
                player = data["players"].get(uid)
                if player:
                    reward = random.randint(60, 120)
                    player["money"] += reward
                    add_xp(player, 12)
            msg = "🛡️ Police raid defeated! Defenders were rewarded."
        else:
            for uid, player in data["players"].items():
                if now_ts() < int(player.get("shield_until", 0)):
                    continue
                inf = int(player.get("influence", 0))
                police_factor = 1.0 + max(0, inf) / 200.0
                loss = int(player.get("money", 0) * 0.12 * police_factor)
                if inf > 50:
                    loss = int(loss * 1.5)
                elif inf < -50:
                    loss = int(loss * 0.7)
                player["money"] = max(0, player["money"] - loss)
                if player.get("joints", 0) > 0 and random.random() < 0.35:
                    player["joints"] -= 1
                if player.get("meth", 0) > 0 and random.random() < 0.20:
                    player["meth"] = max(0, player["meth"] - 1)
                if player.get("salt", 0) > 0 and random.random() < 0.20:
                    player["salt"] = max(0, player["salt"] - 1)
                clamp_player(player)
            msg = "🚓 Police raid succeeded. Unshielded players lost money."

        data["meta"]["active_raid"] = None
        save_data(data)
        for guild in bot.guilds:
            channel = guild.system_channel or next(
                (c for c in guild.text_channels if c.permissions_for(guild.me).send_messages),
                None,
            )
            if channel:
                await channel.send(msg)
        return

    if raid:
        return

    # Start a new raid
    data["meta"]["active_raid"] = {
        "id": str(now_ts()),
        "starts_at": now_ts(),
        "ends_at": now_ts() + 40,
        "fighters": [],
    }
    save_data(data)
    for guild in bot.guilds:
        channel = guild.system_channel or next(
            (c for c in guild.text_channels if c.permissions_for(guild.me).send_messages),
            None,
        )
        if channel:
            await channel.send("🚨 Police raid started! Use `!fightpolice` within 40s.")


# ================================================================
# PyQt5 GUI + bot runner
# ================================================================
class BotRunnerThread(QtCore.QThread):
    status_signal = QtCore.pyqtSignal(str)

    def __init__(self, token: str, guild_id: Optional[int]):
        super().__init__()
        self.token = token
        self.guild_id = guild_id
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.stop_requested = False

    def run(self) -> None:
        global BOT_STATUS_CALLBACK, ALLOWED_GUILD_ID
        self.stop_requested = False
        self.status_signal.emit("Bot starting")
        BOT_STATUS_CALLBACK = self.status_signal.emit
        ALLOWED_GUILD_ID = self.guild_id
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        async def start_bot() -> None:
            had_error = False
            try:
                # Fix "Session is closed" on repeated start attempts in same app session.
                session = getattr(bot.http, "_HTTPClient__session", None)
                if session is not None and getattr(session, "closed", False):
                    setattr(bot.http, "_HTTPClient__session", None)
                await bot.start(self.token)
            except Exception as e:
                had_error = True
                self.status_signal.emit(f"Error: {e}")
                # Ensure aiohttp session is closed on startup/runtime failure.
                if not bot.is_closed():
                    try:
                        await bot.close()
                    except Exception:
                        pass
            finally:
                globals()["BOT_STATUS_CALLBACK"] = None
                globals()["ALLOWED_GUILD_ID"] = None
                if self.stop_requested:
                    self.status_signal.emit("Bot not started")
                elif not had_error:
                    self.status_signal.emit("Bot stopped")

        try:
            self.loop.run_until_complete(start_bot())
        finally:
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            if pending:
                try:
                    self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    pass
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()

    def stop_bot(self) -> None:
        if self.loop and self.loop.is_running():
            self.stop_requested = True
            future = asyncio.run_coroutine_threadsafe(bot.close(), self.loop)
            try:
                future.result(timeout=8)
            except Exception:
                pass


class BotControlWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.runner: Optional[BotRunnerThread] = None
        self.server_process: Optional[subprocess.Popen] = None
        self.tray_icon: Optional[QtWidgets.QSystemTrayIcon] = None
        self.log_timer = QtCore.QTimer(self)
        self.stats_timer = QtCore.QTimer(self)
        self.setup_ui()
        self.setup_tray()
        self.setup_timers()

    def setup_ui(self) -> None:
        self.setWindowTitle("Cannabis Economy Bot")
        self.setFixedSize(980, 680)
        self.setWindowFlags(QtCore.Qt.FramelessWindowHint | QtCore.Qt.WindowStaysOnTopHint)
        self.setAttribute(QtCore.Qt.WA_TranslucentBackground)

        container = QtWidgets.QFrame(self)
        container.setGeometry(10, 10, 960, 660)
        container.setStyleSheet(
            """
            QFrame {
                background: rgba(30, 30, 30, 230);
                border-radius: 14px;
                border: 1px solid rgba(255, 255, 255, 40);
            }
            QLabel { color: white; font-size: 13px; }
            QLineEdit {
                color: white;
                background: rgba(255, 255, 255, 20);
                border-radius: 10px;
                padding: 8px;
                border: 1px solid rgba(255,255,255,50);
            }
            QPushButton {
                color: white;
                background: rgba(67, 160, 71, 180);
                border-radius: 10px;
                padding: 8px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: rgba(92, 190, 96, 210);
            }
            QPushButton#stopBtn {
                background: rgba(198, 40, 40, 180);
            }
            QTabWidget::pane {
                border: 1px solid rgba(255,255,255,40);
                border-radius: 10px;
                padding: 6px;
            }
            QTabBar::tab {
                background: rgba(255,255,255,18);
                color: white;
                padding: 8px 14px;
                margin-right: 6px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
            }
            QTabBar::tab:selected {
                background: rgba(76,175,80,180);
            }
            """
        )

        layout = QtWidgets.QVBoxLayout(container)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(12)

        title = QtWidgets.QLabel("🌿 Discord Cannabis Economy Bot")
        title.setAlignment(QtCore.Qt.AlignCenter)
        title.setFont(QtGui.QFont("Segoe UI", 12, QtGui.QFont.Bold))

        self.token_input = QtWidgets.QLineEdit()
        self.token_input.setPlaceholderText("Enter bot token")
        self.token_input.setEchoMode(QtWidgets.QLineEdit.Password)

        self.guild_input = QtWidgets.QLineEdit()
        self.guild_input.setPlaceholderText("Guild ID (обязательно)")

        self.status_label = QtWidgets.QLabel("Bot not started")
        self.status_label.setAlignment(QtCore.Qt.AlignCenter)

        self.info_box = QtWidgets.QPlainTextEdit()
        self.info_box.setReadOnly(True)
        self.info_box.setPlainText(
            "Инструкция:\n"
            "1) Вставь токен бота.\n"
            "2) Введи Guild ID сервера, где бот должен работать.\n"
            "3) Нажми Start.\n\n"
            "Ссылка для создания приложения:\n"
            "https://discord.com/developers/applications"
        )
        self.info_box.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOn)
        self.info_box.setLineWrapMode(QtWidgets.QPlainTextEdit.WidgetWidth)
        self.info_box.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self.info_box.setFixedHeight(130)

        buttons = QtWidgets.QHBoxLayout()
        self.start_btn = QtWidgets.QPushButton("Start")
        self.stop_btn = QtWidgets.QPushButton("Stop")
        self.stop_btn.setObjectName("stopBtn")
        self.copy_link_btn = QtWidgets.QPushButton("Copy Invite Link")
        self.tray_btn = QtWidgets.QPushButton("Свернуть в трей")
        self.min_btn = QtWidgets.QPushButton("Свернуть окно")
        self.restart_btn = QtWidgets.QPushButton("Перезапуск бота")
        self.refresh_btn = QtWidgets.QPushButton("Обновить данные")
        self.start_server_btn = QtWidgets.QPushButton("Запустить сервер")
        self.stop_server_btn = QtWidgets.QPushButton("Остановить сервер")

        self.start_btn.clicked.connect(self.start_bot)
        self.stop_btn.clicked.connect(self.stop_bot)
        self.copy_link_btn.clicked.connect(self.copy_invite_link)
        self.tray_btn.clicked.connect(self.minimize_to_tray)
        self.min_btn.clicked.connect(self.showMinimized)
        self.restart_btn.clicked.connect(self.restart_bot)
        self.refresh_btn.clicked.connect(self.refresh_data)
        self.start_server_btn.clicked.connect(self.start_local_server)
        self.stop_server_btn.clicked.connect(self.stop_local_server)

        buttons.addWidget(self.start_btn)
        buttons.addWidget(self.stop_btn)
        buttons.addWidget(self.copy_link_btn)
        buttons.addWidget(self.min_btn)
        buttons.addWidget(self.tray_btn)
        buttons.addWidget(self.restart_btn)
        buttons.addWidget(self.refresh_btn)
        buttons.addWidget(self.start_server_btn)
        buttons.addWidget(self.stop_server_btn)

        layout.addWidget(title)
        layout.addWidget(self.token_input)
        layout.addWidget(self.guild_input)
        layout.addLayout(buttons)
        layout.addWidget(self.status_label)
        self.tabs = QtWidgets.QTabWidget()
        dashboard = QtWidgets.QWidget()
        dlay = QtWidgets.QVBoxLayout(dashboard)
        dlay.addWidget(self.info_box)
        self.stats_label = QtWidgets.QLabel("Статистика загрузится...")
        dlay.addWidget(self.stats_label)
        self.tabs.addTab(dashboard, "Dashboard")

        self.log_box = QtWidgets.QTextEdit()
        self.log_box.setReadOnly(True)
        logs_tab = QtWidgets.QWidget()
        llay = QtWidgets.QVBoxLayout(logs_tab)
        llay.addWidget(self.log_box)
        self.tabs.addTab(logs_tab, "Логи")

        settings_tab = QtWidgets.QWidget()
        slay = QtWidgets.QVBoxLayout(settings_tab)
        self.tax_spin = QtWidgets.QSpinBox()
        self.tax_spin.setRange(60, 86400)
        self.tax_spin.setValue(TAX_INTERVAL)
        self.raid_spin = QtWidgets.QSpinBox()
        self.raid_spin.setRange(60, 86400)
        self.raid_spin.setValue(RAID_INTERVAL)
        self.daily_spin = QtWidgets.QSpinBox()
        self.daily_spin.setRange(3600, 172800)
        self.daily_spin.setValue(DAILY_COOLDOWN)
        apply_btn = QtWidgets.QPushButton("Применить настройки")
        apply_btn.clicked.connect(self.apply_runtime_settings)
        slay.addWidget(QtWidgets.QLabel("Tax interval (sec)"))
        slay.addWidget(self.tax_spin)
        slay.addWidget(QtWidgets.QLabel("Raid interval (sec)"))
        slay.addWidget(self.raid_spin)
        slay.addWidget(QtWidgets.QLabel("Daily cooldown (sec)"))
        slay.addWidget(self.daily_spin)
        slay.addWidget(apply_btn)
        self.tabs.addTab(settings_tab, "Настройки")

        self.json_editor = QtWidgets.QPlainTextEdit()
        json_tab = QtWidgets.QWidget()
        jlay = QtWidgets.QVBoxLayout(json_tab)
        save_json_btn = QtWidgets.QPushButton("Сохранить JSON")
        reload_json_btn = QtWidgets.QPushButton("Reload JSON")
        save_json_btn.clicked.connect(self.save_json_editor)
        reload_json_btn.clicked.connect(self.reload_json_editor)
        jlay.addWidget(self.json_editor)
        jlay.addWidget(save_json_btn)
        jlay.addWidget(reload_json_btn)
        self.tabs.addTab(json_tab, "JSON Editor")

        layout.addWidget(self.tabs)
        self.reload_json_editor()
        self.refresh_logs()
        self.refresh_dashboard()

    def update_status(self, text: str) -> None:
        self.status_label.setText(text)
        if "Logged in" in text or "online" in text.lower():
            self.status_label.setText("Bot online ✅")

    def start_bot(self) -> None:
        token = self.token_input.text().strip()
        guild_raw = self.guild_input.text().strip()
        if not token:
            self.status_label.setText("Error")
            return
        if not guild_raw.isdigit():
            self.status_label.setText("Error: Guild ID required")
            return
        if self.runner and self.runner.isRunning():
            self.status_label.setText("Bot already running")
            return

        self.runner = BotRunnerThread(token, int(guild_raw))
        self.runner.status_signal.connect(self.update_status)
        self.runner.start()
        self.status_label.setText("Bot starting")

    def copy_invite_link(self) -> None:
        guild_raw = self.guild_input.text().strip()
        if not guild_raw.isdigit():
            self.status_label.setText("Error: Guild ID required")
            return

        if bot.user is None:
            # Fallback to dev portal link if bot is not logged in yet.
            link = "https://discord.com/developers/applications"
        else:
            permissions = 8
            link = (
                f"https://discord.com/oauth2/authorize?client_id={bot.user.id}"
                f"&permissions={permissions}&scope=bot%20applications.commands&guild_id={guild_raw}"
            )
        QtWidgets.QApplication.clipboard().setText(link)
        self.status_label.setText("Link copied ✅")

    def stop_bot(self) -> None:
        if self.runner and self.runner.isRunning():
            self.runner.stop_bot()
            self.runner.wait(3000)
            self.status_label.setText("Bot not started")
        else:
            self.status_label.setText("Bot not started")

    def restart_bot(self) -> None:
        self.stop_bot()
        self.start_bot()

    def refresh_data(self) -> None:
        self.reload_json_editor()
        self.refresh_logs()
        self.refresh_dashboard()
        self.status_label.setText("Данные обновлены")

    def setup_timers(self) -> None:
        self.log_timer.timeout.connect(self.refresh_logs)
        self.log_timer.start(2000)
        self.stats_timer.timeout.connect(self.refresh_dashboard)
        self.stats_timer.start(3000)

    def start_local_server(self) -> None:
        if self.server_process and self.server_process.poll() is None:
            self.status_label.setText("Сервер уже запущен")
            return
        try:
            self.server_process = subprocess.Popen([sys.executable, "server.py"])
            self.status_label.setText("Сервер запущен")
            log_event("SERVER", "system", "local server started")
        except Exception as e:
            self.status_label.setText(f"Ошибка сервера: {e}")

    def stop_local_server(self) -> None:
        if self.server_process and self.server_process.poll() is None:
            self.server_process.terminate()
            self.status_label.setText("Сервер остановлен")
            log_event("SERVER", "system", "local server stopped")
        else:
            self.status_label.setText("Сервер не запущен")

    def reload_json_editor(self) -> None:
        data = load_data()
        self.json_editor.setPlainText(json.dumps(data, ensure_ascii=False, indent=2))

    def save_json_editor(self) -> None:
        try:
            data = json.loads(self.json_editor.toPlainText() or "{}")
            save_data(data)
            self.status_label.setText("JSON сохранён")
            self.refresh_logs()
        except Exception as e:
            self.status_label.setText(f"Ошибка JSON: {e}")

    def refresh_logs(self) -> None:
        if not os.path.exists(LOG_FILE):
            self.log_box.setHtml("")
            return
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()[-400:]
        html_lines = []
        for line in lines:
            color = "#C8E6C9"
            if "ERROR" in line or "FAIL" in line:
                color = "#FF8A80"
            elif "WARN" in line:
                color = "#FFD180"
            elif "RAID" in line:
                color = "#B39DDB"
            html_lines.append(f'<span style="color:{color}">{line.rstrip()}</span>')
        self.log_box.setHtml("<br>".join(html_lines))
        self.log_box.moveCursor(QtGui.QTextCursor.End)

    def refresh_dashboard(self) -> None:
        try:
            data = load_data()
            players = data.get("players", {})
            online = len(players)
            total_money = sum(int(p.get("money", 0)) for p in players.values())
            active_raids = 1 if data.get("meta", {}).get("active_raid") else 0
            self.stats_label.setText(
                f"Игроков: {online} | Всего денег: ${total_money} | Активных рейдов: {active_raids}"
            )
        except Exception as e:
            self.stats_label.setText(f"Ошибка Dashboard: {e}")

    def apply_runtime_settings(self) -> None:
        global TAX_INTERVAL, RAID_INTERVAL, DAILY_COOLDOWN
        TAX_INTERVAL = int(self.tax_spin.value())
        RAID_INTERVAL = int(self.raid_spin.value())
        DAILY_COOLDOWN = int(self.daily_spin.value())
        self.status_label.setText("Runtime настройки применены")
        log_event("SETTINGS", "system", f"TAX={TAX_INTERVAL}, RAID={RAID_INTERVAL}, DAILY={DAILY_COOLDOWN}")

    def setup_tray(self) -> None:
        if not QtWidgets.QSystemTrayIcon.isSystemTrayAvailable():
            return
        icon = self.style().standardIcon(QtWidgets.QStyle.SP_ComputerIcon)
        self.tray_icon = QtWidgets.QSystemTrayIcon(icon, self)
        menu = QtWidgets.QMenu()
        open_action = menu.addAction("Открыть")
        exit_action = menu.addAction("Выход")
        open_action.triggered.connect(self.restore_from_tray)
        exit_action.triggered.connect(QtWidgets.QApplication.instance().quit)
        self.tray_icon.setContextMenu(menu)
        self.tray_icon.activated.connect(self.on_tray_activated)
        self.tray_icon.show()

    def minimize_to_tray(self) -> None:
        if self.tray_icon:
            self.hide()
            self.tray_icon.showMessage("WeedSimulator 4.0", "Окно свернуто в трей.", QtWidgets.QSystemTrayIcon.Information, 2000)

    def restore_from_tray(self) -> None:
        self.show()
        self.raise_()
        self.activateWindow()

    def on_tray_activated(self, reason: QtWidgets.QSystemTrayIcon.ActivationReason) -> None:
        if reason == QtWidgets.QSystemTrayIcon.DoubleClick:
            self.restore_from_tray()

    def mousePressEvent(self, event: QtGui.QMouseEvent) -> None:
        if event.button() == QtCore.Qt.LeftButton:
            self._drag_pos = event.globalPos() - self.frameGeometry().topLeft()
            event.accept()

    def mouseMoveEvent(self, event: QtGui.QMouseEvent) -> None:
        if event.buttons() == QtCore.Qt.LeftButton:
            self.move(event.globalPos() - self._drag_pos)
            event.accept()


def main() -> None:
    # Ensure persistent storage exists before startup
    save_data(load_data())

    app = QtWidgets.QApplication(sys.argv)
    window = BotControlWindow()
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
