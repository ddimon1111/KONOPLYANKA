import asyncio
import json
import os
import random
import sys
import threading
import time
from typing import Any, Dict, List, Optional

import discord
from discord.ext import commands, tasks
from PyQt5 import QtCore, QtGui, QtWidgets

# ================================================================
# Configuration
# ================================================================
DATA_FILE = "game_data.json"
BOT_PREFIX = "!"

PLANT_COOLDOWN = 30
DAILY_COOLDOWN = 30
SMOKE_DURATION = 120
PLANT_BASE_GROWTH = 30
AUTO_INCOME_INTERVAL = 60
TAX_INTERVAL = 180
RAID_INTERVAL = 120

BASE_JOINT_PRICE = 35
XP_PER_ACTION = 8
XP_LEVEL_SCALE = 120

# Contraband configuration: duration in seconds, min/max reward
CONTRABAND_COUNTRIES: Dict[str, Dict[str, int]] = {
    "china": {"duration": 50, "min": 150, "max": 280},
    "russia": {"duration": 40, "min": 120, "max": 240},
    "ukraine": {"duration": 35, "min": 110, "max": 220},
    "canada": {"duration": 30, "min": 95, "max": 180},
    "usa": {"duration": 28, "min": 90, "max": 170},
    "brazil": {"duration": 45, "min": 130, "max": 260},
    "venezuela": {"duration": 55, "min": 170, "max": 320},
}

RARITY_TABLE: List[Dict[str, Any]] = [
    {"name": "common", "weight": 55, "yield_min": 2, "yield_max": 4},
    {"name": "rare", "weight": 28, "yield_min": 4, "yield_max": 7},
    {"name": "epic", "weight": 13, "yield_min": 7, "yield_max": 11},
    {"name": "legendary", "weight": 4, "yield_min": 11, "yield_max": 16},
]

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


# ================================================================
# Utility
# ================================================================
def now_ts() -> int:
    return int(time.time())


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
        "farm": None,
        "contraband": None,
    }


def load_data() -> Dict[str, Any]:
    with DATA_LOCK:
        if not os.path.exists(DATA_FILE):
            data = ensure_shape({})
            save_data(data)
            return data
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}
        return ensure_shape(data)


def save_data(data: Dict[str, Any]) -> None:
    with DATA_LOCK:
        tmp = f"{DATA_FILE}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, DATA_FILE)


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
    lamp_reduction = min(8, int(player.get("inventory", {}).get("lamps", 0)))
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
    await ctx.send(tr(player, "menu_title"), view=GameMenuView())


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
    inv = player.get("inventory", {})
    text = (
        f"{tr(player, 'inventory')}\n"
        f"💡 Lamps: {inv.get('lamps', 0)}\n"
        f"🧪 Fertilizers: {player.get('fertilizers', 0)}\n"
        f"⚗️ Chemicals: {player.get('chemicals', 0)}"
    )
    save_data(data)
    await ctx.send(text)


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
async def sell_cmd(ctx: commands.Context, amount: int) -> None:
    if amount <= 0:
        await ctx.send("Amount must be positive.")
        return
    data = load_data()
    player = get_player(data, ctx.author)
    if player["joints"] < amount:
        await ctx.send("Not enough joints.")
        return
    price_per = BASE_JOINT_PRICE + player["level"] * 2
    total = amount * price_per
    player["joints"] -= amount
    player["money"] += total
    add_xp(player, XP_PER_ACTION)
    save_data(data)
    await ctx.send(f"💰 Sold {amount} joints for ${total}.")


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
    data["cartels"][key] = {"name": name, "owner": str(ctx.author.id), "members": [str(ctx.author.id)], "bank": 0}
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
    await ctx.send(embed=embed)


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
    await ctx.send(msg)


@bot.command(name="help")
async def help_cmd(ctx: commands.Context) -> None:
    await ctx.send(
        "Команды: !menu !about !plant !harvest !dry !roll !smoke !balance "
        "!sell <amount> !duel @user <amount> !casino <amount> !daily !fightpolice "
        "!farm create/join/info !farmrank !upgrade !exchange <joints> !contraband <country> "
        "!leaderboard !trade @user <joints> !water !care !autowater on/off "
        "!buyfert <n> !buychem <n> !inventory !auction create/list/bid/claim "
        "!cartel ... !reputation !casinotour ... !betduel !resolveduel"
    )


# ================================================================
# Background loops
# ================================================================
@tasks.loop(seconds=AUTO_INCOME_INTERVAL)
async def auto_income_loop() -> None:
    data = load_data()
    for player in data["players"].values():
        base = 15 + player["level"] * 2 + player["upgrades"]["farm_level"] * 3
        if now_ts() < int(player.get("smoke_until", 0)):
            base += 10
        if player.get("auto_watering") and player["plants"]:
            for plant in player["plants"]:
                plant["last_watered"] = now_ts()
            base -= 8
        player["money"] += base
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
                loss = int(player.get("money", 0) * 0.12)
                player["money"] = max(0, player["money"] - loss)
                if player.get("joints", 0) > 0 and random.random() < 0.35:
                    player["joints"] -= 1
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
        self.setup_ui()

    def setup_ui(self) -> None:
        self.setWindowTitle("Cannabis Economy Bot")
        self.setFixedSize(520, 420)
        self.setWindowFlags(QtCore.Qt.FramelessWindowHint | QtCore.Qt.WindowStaysOnTopHint)
        self.setAttribute(QtCore.Qt.WA_TranslucentBackground)

        container = QtWidgets.QFrame(self)
        container.setGeometry(10, 10, 500, 400)
        container.setStyleSheet(
            """
            QFrame {
                background: rgba(30, 30, 30, 220);
                border-radius: 20px;
                border: 1px solid rgba(255, 255, 255, 40);
            }
            QLabel { color: white; font-size: 14px; }
            QLineEdit {
                color: white;
                background: rgba(255, 255, 255, 20);
                border-radius: 12px;
                padding: 8px;
                border: 1px solid rgba(255,255,255,50);
            }
            QPushButton {
                color: white;
                background: rgba(67, 160, 71, 180);
                border-radius: 12px;
                padding: 8px;
                font-weight: 600;
            }
            QPushButton#stopBtn {
                background: rgba(198, 40, 40, 180);
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
        self.info_box.setFixedHeight(130)

        buttons = QtWidgets.QHBoxLayout()
        self.start_btn = QtWidgets.QPushButton("Start")
        self.stop_btn = QtWidgets.QPushButton("Stop")
        self.stop_btn.setObjectName("stopBtn")
        self.copy_link_btn = QtWidgets.QPushButton("Copy Invite Link")

        self.start_btn.clicked.connect(self.start_bot)
        self.stop_btn.clicked.connect(self.stop_bot)
        self.copy_link_btn.clicked.connect(self.copy_invite_link)

        buttons.addWidget(self.start_btn)
        buttons.addWidget(self.stop_btn)
        buttons.addWidget(self.copy_link_btn)

        layout.addWidget(title)
        layout.addWidget(self.token_input)
        layout.addWidget(self.guild_input)
        layout.addLayout(buttons)
        layout.addWidget(self.status_label)
        layout.addWidget(self.info_box)

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
