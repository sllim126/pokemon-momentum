"""Auditable market-index definitions shared by builders, validation, and routes."""

SV100_GROUP_IDS = [
    24325, 24326, 24269, 24073, 23821, 23651, 23537, 23529,
    23473, 23381, 23353, 23286, 23237, 23228, 23120, 22873,
]

# Contract:
# - group_ids: exact set universe for intentionally narrow historical indexes.
# - generation: category-specific era bucket resolved from set metadata.
# - exclude_group_ids: preconstructed, reprint, or catch-all buckets outside the
#   expansion-card scope of generation indexes.
# - all_active_groups: global market index across every group with price data.
# - constituent_limit: exact latest-day holdings target.
INDEX_DEFINITIONS = {
    "pokemon100": {
        "index_name": "Pokemon Top 151",
        "description": "Top 151 cards by market price (all English sets)",
        "base_level": 1000.0,
        "all_active_groups": True,
        "constituent_limit": 151,
        "release_markers_enabled": False,
    },
    "sv100": {
        "index_name": "Scarlet & Violet 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "group_ids": SV100_GROUP_IDS,
        "release_markers_enabled": True,
    },
    "mega100": {
        "index_name": "Mega Evolution 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "generation": "MEG",
        "release_markers_enabled": True,
    },
    "swsh100": {
        "index_name": "Sword & Shield 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "generation": "SWSH",
        "exclude_group_ids": [2686, 3051],  # Battle Academy products.
        "release_markers_enabled": False,
    },
    "sm100": {
        "index_name": "Sun & Moon 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "generation": "SM",
        "exclude_group_ids": [2069, 2208, 2282, 23095],
        "release_markers_enabled": False,
    },
    "xy100": {
        "index_name": "XY 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "generation": "XY",
        "exclude_group_ids": [1528, 1532, 1533, 1536, 1539, 1796, 1840],
        "release_markers_enabled": False,
    },
    "bw100": {
        "index_name": "Black & White 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "generation": "BW",
        "exclude_group_ids": [1538],  # BW Trainer Kit.
        "release_markers_enabled": False,
    },
    "dp100": {
        "index_name": "Diamond & Pearl / Platinum / HGSS 100",
        "description": "Top 100 cards by market price across the DP, Platinum, and HGSS eras",
        "base_level": 1000.0,
        "generation": "DP/HGSS",
        "exclude_group_ids": [1540, 1541],  # HGSS and DP Trainer Kits.
        "release_markers_enabled": False,
    },
    "ex100": {
        "index_name": "EX 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "generation": "EX",
        "exclude_group_ids": [1542, 1543, 1853],  # Trainer Kits and EX Battle Stadium.
        "release_markers_enabled": False,
    },
    "wotc100": {
        "index_name": "Original WOTC 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "group_ids": [604, 1663, 635, 630, 605, 1373, 1441, 1440],
        "release_markers_enabled": False,
    },
    "neo100": {
        "index_name": "Neo 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "group_ids": [1396, 1434, 1389, 1444],
        "release_markers_enabled": False,
    },
    "ecard100": {
        "index_name": "e-Card 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "group_ids": [1375, 1397, 1372],
        "release_markers_enabled": False,
    },
    "jp_pokemon100": {
        "index_name": "JP Pokemon Top 151",
        "description": "Top 151 cards by market price (all active Japanese sets)",
        "base_level": 1000.0,
        "all_active_groups": True,
        "constituent_limit": 151,
        "release_markers_enabled": False,
        "category_id": 85,
    },
    "jp_sv100": {
        "index_name": "JP Scarlet & Violet 100",
        "description": "Top 100 cards by market price",
        "base_level": 1000.0,
        "generation": "SV",
        "release_markers_enabled": True,
        "category_id": 85,
    },
}


def index_keys_for_category(category_id: int) -> list[str]:
    requested = int(category_id)
    return [
        key
        for key, definition in INDEX_DEFINITIONS.items()
        if int(definition.get("category_id") or 3) == requested
    ]
