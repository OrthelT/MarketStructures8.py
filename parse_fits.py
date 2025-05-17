import re
from re import search
from dataclasses import dataclass, field
from typing import Optional, Generator, List
from collections import defaultdict
from datetime import datetime

import pandas as pd
from numpy import unique
from sqlalchemy import create_engine, text, insert, delete, select
from sqlalchemy.orm import sessionmaker, Session

import logging_tool
from data_mapping import map_data, remap_reversable
from models import JoinedInvTypes, Fittings_FittingItem, Fittings_Fitting

fittings_fittingitem = Fittings_FittingItem

fittings_db = r"mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"
mkt_db = "sqlite:///market_orders.sqlite"
sde_db = r"sqlite:///C:/Users/User/PycharmProjects/ESI_Utilities/SDE/SDE sqlite-latest.sqlite"

from logging import getLogger

logger = logging_tool.configure_logging(log_name=__name__)

@dataclass
class FittingItem:
    flag: str
    quantity: int
    fit_id: int
    type_name: str
    ship_type_name: str
    fit_name: Optional[str] = None

    # Declare attributes that will be assigned in __post_init__
    type_id: int = field(init=False)
    type_fk_id: int = field(init=False)

    def __post_init__(self) -> None:
        self.type_id = self.get_type_id()
        self.type_fk_id = self.type_id  # optional alias
        self.details = self.get_fitting_details()
        self.description = self.details['description']

        # Only set it if it's not already passed from EFT
        if self.fit_name is None:
            if "name" in self.details:
                self.fit_name = self.details["name"]
                if "name" in self.details and self.fit_name != self.details["name"]:
                    logger.warning(
                        f"Fit name mismatch: parsed='{self.fit_name}' vs DB='{self.details['name']}'"
                    )
            else:
                self.fit_name = f"Default {self.ship_type_name} fit"

    def get_type_id(self) -> int:
        engine = create_engine(fittings_db, echo=False)
        query = text("SELECT type_id FROM fittings_type WHERE type_name = :type_name")
        with engine.connect() as conn:
            result = conn.execute(query, {"type_name": self.type_name}).fetchone()
            return result[0] if result else -1  # return a sentinel or raise error

    def get_fitting_details(self) -> dict:
        engine = create_engine(fittings_db, echo=False)
        query = text("SELECT * FROM fittings_fitting WHERE id = :fit_id")
        with engine.connect() as conn:
            row = conn.execute(query, {"fit_id": self.fit_id}).fetchone()
            return dict(row._mapping) if row else {}

#Utility functions
def convert_fit_date(date: str) -> datetime:
    """enter date from WC Auth in format: dd Mon YYYY HH:MM:SS
        Example: 15 Jan 2025 19:12:04
    """

    dt = datetime.strptime("15 Jan 2025 19:12:04", "%d %b %Y %H:%M:%S")
    return dt

def slot_yielder() -> Generator[str, None, None]:
    """
    Yields EFT slot flags in correct order.
    Once primary sections are consumed, defaults to 'Cargo'.
    """
    corrected_order = ['LoSlot', 'MedSlot', 'HiSlot', 'RigSlot', 'DroneBay']
    for slot in corrected_order:
        yield slot
    while True:
        yield 'Cargo'

#EFT Fitting Parser

def process_fit(fit_file: str, fit_id: int) -> List[FittingItem]:
    """
    pass in the path to an EFT formatted fitting file and a fit_id. Returns the fitting items as
    in a list suitable for updating the database.

    :param fit_file:
    :param fit_id:
    :return: List[FittingItem]

    Usage: fit_items = process_fit("drake2501_39.txt", fit_id=39)

    """

    fit = []
    qty = 1
    slot_gen = slot_yielder()
    current_slot = None
    ship_name = ""
    fit_name = ""
    slot_counters = defaultdict(int)

    with open(fit_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()

            if line.startswith("[") and line.endswith("]"):
                clean_name = line.strip('[]')
                parts = clean_name.split(',')
                ship_name = parts[0].strip()
                fit_name = parts[1].strip() if len(parts) > 1 else "Unnamed Fit"
                continue

            if line == "":
                # Only advance to the next slot when a blank line *after* content is found
                current_slot = next(slot_gen)
                continue

            if current_slot is None:
                # First block: assign the first slot only when we encounter the first item
                current_slot = next(slot_gen)

            # Parse quantity
            qty_match = re.search(r'\s+x(\d+)$', line)
            if qty_match:
                qty = int(qty_match.group(1))
                item = line[:qty_match.start()].strip()
            else:
                qty = 1
                item = line.strip()

            # Construct slot name
            if current_slot in {'LoSlot', 'MedSlot', 'HiSlot', 'RigSlot'}:
                suffix = slot_counters[current_slot]
                slot_counters[current_slot] += 1
                slot_name = f"{current_slot}{suffix}"
            else:
                slot_name = current_slot  # 'DroneBay' or 'Cargo'

            fitting_item = FittingItem(
                flag=slot_name,
                fit_id=fit_id,
                type_name=item,
                ship_type_name=ship_name,
                fit_name=fit_name,
                quantity=qty
            )

            fit.append(fitting_item)
        return fit

#Database update functions

def insert_fittings_fittingitems(fit_items: List[FittingItem], session: Session):
    stmt = insert(Fittings_FittingItem).values([
        {
            "flag": item.flag,
            "quantity": item.quantity,
            "type_id": item.type_id,
            "fit_id": item.fit_id,
            "type_fk_id": item.type_fk_id
        }
        for item in fit_items
    ])
    session.execute(stmt)
    session.commit()

def update_fittings_fitting(
    fit_items: List[FittingItem],
    session: Session,
    last_updated: Optional[datetime] = None
):

    """ Database update for the fittings_fitting table. Takes the output of process_fit,
        a SQLAlchemy session, and datetime formatted update time. The convert_fit_date() function allows
        last update time from WC Auth fittings.

            converted_time = convert_fit_date('15 Jan 2025 19:12:04')
            Usage:  update_fittings_fitting(fit_items=fit_items, session=session, last_updated=converted_time)
        """

    if not fit_items:
        print("⚠️ No items provided. Skipping update.")
        return

    # fit_id is the primary key of fittings_fitting
    fit_id = fit_items[0].fit_id
    fit_name = fit_items[0].fit_name
    ship_name = fit_items[0].ship_type_name

    # Derive the description text (human-readable inventory)
    description = "\n".join(
        f"{item.flag} x{item.quantity} {item.type_name}" for item in fit_items
    )

    # Look up the ship's type_id from fittings_type
    engine = create_engine(fittings_db, echo=False)
    query = text("SELECT type_id FROM fittings_type WHERE type_name = :ship")
    with engine.connect() as conn:
        result = conn.execute(query, {"ship": ship_name}).fetchone()
        ship_type_id = result[0] if result else None

    if ship_type_id is None:
        raise ValueError(f"❌ Ship type '{ship_name}' not found in fittings_type.")

    # Fetch existing fitting record from DB by ID (== fit_id)
    existing = session.get(Fittings_Fitting, fit_id)
    if not existing:
        raise ValueError(f"❌ Fit with id={fit_id} does not exist in fittings_fitting.")

    # Show proposed changes to the user
    print("\n🔁 Proposed update to fittings_fitting:")
    print(f"  ID:              {fit_id}")
    print(f"  Name:            {existing.name} → {fit_name}")
    print(f"  Description:     (will be regenerated from fitting items)")
    print(f"  Ship Type ID:    {existing.ship_type_id} → {ship_type_id}")
    print(f"  Last Updated:    {existing.last_updated} → {last_updated or 'NOW'}\n")

    confirm = input("Apply these changes? (y/n): ").strip().lower()
    if confirm != "y":
        print("🚫 Aborted by user. No changes made.")
        return

    # Apply updates
    existing.name = fit_name
    existing.description = description
    existing.ship_type_type_id = ship_type_id
    existing.ship_type_id = ship_type_id
    existing.last_updated = last_updated or datetime.now()

    session.commit()
    print(f"✅ Successfully updated fit ID {fit_id}.")

if __name__ == '__main__':

    #update with EFT formatted text file
    fit_file = "drake2501_39.txt"

    # # Create engine
    engine = create_engine(fittings_db, echo=False)
    # Create configured session factory

    #update with date from WC Auth fitting
    last_updated = convert_fit_date("15 Jan 2025 19:12:04")

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    fit_items = process_fit(fit_file, fit_id=39)
    for item in fit_items:
        print(item)

    update_fittings_fitting(fit_items=fit_items, session=session, last_updated=last_updated)