"""
Catalog of Austrian railway stations (ÖBB network).

IDs follow the EVA standard (the same used by DB/ÖBB in the UIC space) with
approximate coordinates. A curated list of real stations — used by the
simulator to generate plausible trips across Austria.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Station:
    eva_id: str
    name: str
    lat: float
    lon: float


# Real stations of the Austrian network (representative sample of the big hubs).
AUSTRIAN_STATIONS: list[Station] = [
    Station("8103000", "Wien Hauptbahnhof", 48.1855, 16.3776),
    Station("8100003", "Wien Meidling", 48.1745, 16.3320),
    Station("8101003", "Wien Westbahnhof", 48.1967, 16.3380),
    Station("8100173", "Salzburg Hbf", 47.8130, 13.0457),
    Station("8100002", "Innsbruck Hbf", 47.2632, 11.4010),
    Station("8100013", "Linz/Donau Hbf", 48.2906, 14.2918),
    Station("8100008", "Graz Hbf", 47.0727, 15.4160),
    Station("8100108", "Klagenfurt Hbf", 46.6166, 14.3097),
    Station("8100053", "Villach Hbf", 46.6128, 13.8430),
    Station("8100206", "Wels Hbf", 48.1666, 14.0289),
    Station("8100025", "St. Pölten Hbf", 48.2080, 15.6248),
    Station("8100063", "Bregenz", 47.5030, 9.7395),
    Station("8100098", "Wiener Neustadt Hbf", 47.8160, 16.2300),
    Station("8100048", "Leoben Hbf", 47.3820, 15.0980),
    Station("8100068", "Feldkirch", 47.2390, 9.6020),
    Station("8100510", "Amstetten", 48.1230, 14.8720),
    Station("8100128", "Bischofshofen", 47.4170, 13.2200),
    Station("8100164", "Saalfelden", 47.4270, 12.8480),
    Station("8100096", "Spittal-Millstättersee", 46.7960, 13.4930),
    Station("8100046", "Knittelfeld", 47.2160, 14.8290),
    Station("8100012", "Attnang-Puchheim", 48.0140, 13.7150),
    Station("8100154", "Schwarzach-St. Veit", 47.3200, 13.1530),
    Station("8100173b", "Hallein", 47.6830, 13.0960),
    Station("8100517", "Tulln an der Donau", 48.3320, 16.0570),
    Station("8100009", "Bruck an der Mur", 47.4100, 15.2730),
]


STATION_BY_ID = {s.eva_id: s for s in AUSTRIAN_STATIONS}
