from __future__ import annotations

import shutil
from pathlib import Path


ROOT = Path("/opt/pokemon-momentum")
SOURCE_DIR = ROOT / "images" / "set logos"
TARGET_DIR = ROOT / "images" / "logos"


# One source logo can intentionally fan out to multiple related set/group ids
# when the special subset uses the same visual brand as its parent release.
# The dashboard and set explorer both look for /images/logos/<groupId>.<ext>.
LOGO_MAP = {
    "Base_Set_2_Logo.png": [605],
    "Jungle_Logo.png": [635],
    "Fossil_Logo.png": [630],
    "Team_Rocket_Logo.png": [1373],
    "1600px-Gym_Heroes_Logo.png": [1441],
    "Gym_Challenge_Logo.png": [1440],
    "Neo_Genesis_Logo_EN.png": [1396],
    "1600px-Neo_Discovery_Logo_EN.png": [1434],
    "1358px-Neo_Revelation_Logo_EN.png": [1389],
    "1599px-Neo_Destiny_Logo_EN.png": [1444],
    "1599px-Legendary_Collection_Logo.png": [1374],
    "E1_Logo_EN.png": [1375],
    "1599px-E2_Logo_EN.png": [1397],
    "1598px-E3_Logo_EN.png": [1372],
    "EX1_Logo_EN.png": [1393],
    "EX2_Logo_EN.png": [1392],
    "EX3_Logo_EN.png": [1376],
    "EX4_Logo_EN.png": [1377],
    "EX5_Logo_EN.png": [1416],
    "EX6_Logo_EN.png": [1419],
    "EX7_Logo_EN.png": [1428],
    "EX8_Logo_EN.png": [1404],
    "EX9_Logo_EN.png": [1410],
    "EX10_Logo_EN.png": [1398],
    "EX11_Logo_EN.png": [1429],
    "EX12_Logo_EN.png": [1378],
    "EX13_Logo_EN.png": [1379],
    "EX14_Logo_EN.png": [1395],
    "EX15_Logo_EN.png": [1411],
    "EX16_Logo_EN.png": [1383],
    "DP1_Logo_EN.png": [1430],
    "DP2_Logo_EN.png": [1368],
    "DP3_Logo_EN.png": [1380],
    "DP4_Logo_EN.png": [1405],
    "DP5_Logo_EN.png": [1390],
    "DP6_Logo_EN.png": [1417],
    "DP7_Logo_EN.png": [1369],
    "PL1_Logo_EN.png": [1406],
    "PL2_Logo_EN.png": [1367],
    "PL3_Logo_EN.png": [1384],
    "PL4_Logo_EN.png": [1391],
    "HS1_Logo_EN.png": [1402],
    "HS2_Logo_EN.png": [1399],
    "HS3_Logo_EN.png": [1403],
    "HS4_Logo_EN.png": [1381],
    "BW1_Logo_EN.png": [1400],
    "BW2_Logo_EN.png": [1424],
    "BW3_Logo_EN.png": [1385],
    "BW4_Logo_EN.png": [1412],
    "BW5_Logo_EN.png": [1386],
    "BW6_Logo_EN.png": [1394],
    "BW7_Logo_EN.png": [1408],
    "BW8_Logo_EN.png": [1413],
    "BW9_Logo_EN.png": [1382],
    "BW10_Logo_EN.png": [1370],
    "BW11_Logo_EN.png": [1409, 1465],
    "Dragon_Vault_Logo_EN.png": [1426],
    "XY1_Logo_EN.png": [1387],
    "XY2_Logo_EN.png": [1464],
    "XY3_Logo_EN.png": [1481],
    "XY4_Logo_EN.png": [1494],
    "XY5_Logo_EN.png": [1509],
    "Double_Crisis_Logo_EN.png": [1525],
    "XY6_Logo_EN.png": [1534],
    "XY7_Logo_EN.png": [1576],
    "XY8_Logo_EN.png": [1661],
    "XY9_Logo_EN.png": [1701],
    "Generations_Logo_EN.png": [1728, 1729],
    "XY10_Logo_EN.png": [1780],
    "XY11_Logo_EN.png": [1815],
    "XY12_Logo_EN.png": [1842],
    "Kalos_Starter_Set_Logo_EN.png": [1522],
    "SM1_Logo_EN.png": [1863],
    "SM2_Logo_EN.png": [1919],
    "SM3_Logo_EN.png": [1957],
    "Shining_Legends_Logo_EN.png": [2054],
    "SM4_Logo_EN.png": [2071],
    "SM5_Logo_EN.png": [2178],
    "SM6_Logo_EN.png": [2209],
    "SM7_Logo_EN.png": [2278],
    "Dragon_Majesty_Logo_EN.png": [2295],
    "SM8_Logo_EN.png": [2328],
    "SM9_Logo_EN.png": [2377],
    "SM10_Logo_EN.png": [2420],
    "SM11_Logo_EN.png": [2464],
    "Hidden_Fates_Logo_EN.png": [2480, 2594],
    "SM12_Logo_EN.png": [2534],
    "1600px-Detective_Pikachu_movie_logo.png": [2409],
    "SWSH1_Logo_EN.png": [2585],
    "SWSH2_Logo_EN.png": [2626],
    "SWSH3_Logo_EN.png": [2675],
    "Champion_Path_Logo_EN.png": [2685],
    "SWSH4_Logo_EN.png": [2701],
    "Shining_Fates_Logo_EN.png": [2754, 2781],
    "SWSH5_Logo_EN.png": [2765],
    "SWSH6_Logo_EN.png": [2807],
    "SWSH7_Logo_EN.png": [2848],
    "Celebrations_Logo_EN.png": [2867, 2931],
    "SWSH8_Logo_EN.png": [2906],
    "SWSH9_Logo_EN.png": [2948, 3020],
    "Pokemon_Go_Logo.png": [3064],
    "SWSH10_Logo_EN.png": [3040, 3068],
    "SWSH11_Logo_EN.png": [3118, 3172],
    "SWSH12_Logo_EN.png": [3170, 17674],
    "Crown_Zenith_Logo_EN.png": [17688, 17689],
    "SV1_Logo_EN.png": [22873],
    "SV2_Logo_EN.png": [23120],
    "SV3_Logo_EN.png": [23228],
    "SV3.5_Logo_EN.png": [23237],
    "SV4_Logo_EN.png": [23286],
    "SV4.5_Logo_EN.png": [23353],
    "SV5_Logo_EN.png": [23381],
    "SV6_Logo_EN.png": [23473],
    "1600px-SV6.5_Logo_EN.png": [23529],
    "1599px-SV7_Logo_EN.png": [23537],
    "SV8_Logo_EN.png": [23651],
    "1600px-SV8.5_Logo_EN.png": [23821],
    "1598px-SV9_Logo_EN.png": [24073],
    "1600px-SV10_Logo_EN.png": [24269],
    "1600px-SV10.5_BLK_Logo_EN.png": [24325],
    "1599px-SV10.5_WHT_Logo_EN.png": [24326],
    "1600px-ME1_Logo_EN.png": [24380],
    "1600px-ME2_Logo_EN.png": [24448],
    "1600px-ME2.5_Logo_EN.png": [24541],
    "1600px-ME3_Logo_EN.png": [24587],
    "1598px-ME4_Logo_EN.png": [24655],
    "1600px-SouthernIslandsLogo.png": [648],
    "1599px-Pokémon_Rumble_logo.png": [1433],
}


def main() -> int:
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    copied = 0
    missing = []

    for source_name, group_ids in LOGO_MAP.items():
        source_path = SOURCE_DIR / source_name
        if not source_path.exists():
            missing.append(source_name)
            continue
        ext = source_path.suffix.lower()
        for group_id in group_ids:
            target_path = TARGET_DIR / f"{group_id}{ext}"
            shutil.copy2(source_path, target_path)
            copied += 1

    print(f"Copied {copied} logo files into {TARGET_DIR}")
    if missing:
        print("\nMissing source files:")
        for name in missing:
            print(f"- {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
