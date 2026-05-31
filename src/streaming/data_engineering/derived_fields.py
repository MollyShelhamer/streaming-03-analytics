"""src/streaming/data_engineering/derived_fields.py.

Derived field calculations for sales messages.

Contains functions that compute fields not present in the raw Kafka message.
These fields are calculated by the consumer from the raw message fields
and the reference data (regions.csv).

This file provides a few examples of derived field calculations,
but you are encouraged to add more as needed.

We add total_price, tax_amount, and total as derived fields in this example.

The producer sends raw measurements only.
The consumer is responsible for all derived calculations.

Author: Denise Case
Date: 2026-05

OBS:
  You can add functions and extend this file OR
  copy it to your own module and modify your copy.
"""

# === DECLARE IMPORTS ===

import logging
from typing import Any, Final

# === DECLARE EXPORTS ===

# Use the built-in __all__ variable to declare a list of
# public objects that this module exports.
# This is a common Python convention that helps other developers understand
# which functions are intended for use outside this module.

__all__ = [
    "TAX_RATE_DEFAULT",
    "compute_discount_amount",
    "compute_tax_amount",
    "compute_total_price",
    "enrich_message",
    "get_discount_rate",
    "get_tax_rate",
]

# === DECLARE CONSTANTS ===

# Fallback tax rate used when a region is not found in the lookup table.
TAX_RATE_DEFAULT: Final[float] = 0.08

# === CONFIGURE LOGGER ONCE PER PYTHON FILE (MODULE) ===

LOG = logging.getLogger(__name__)

# === DEFINE DERIVED FIELD FUNCTIONS ===


def compute_total_price(quantity: int, unit_price: float) -> float:
    """Compute the total price before tax.

    Arguments:
        quantity: Number of units purchased.
        unit_price: Price per unit in the order currency.

    Returns:
        Total price rounded to 2 decimal places.
    """
    return round(quantity * unit_price, 2)


def compute_discount_amount(total_price: float, discount_rate: float) -> float:
    """Compute the discount amount for an order.

    Arguments:
        total_price: Total price before discount.
        discount_rate: Discount rate as a decimal (e.g. 0.10 for 10%).

    Returns:
        Discount amount rounded to 2 decimal places.
    """
    return round(total_price * discount_rate, 2)


def compute_tax_amount(total_price: float, tax_rate: float) -> float:
    """Compute the tax amount for an order.

    Arguments:
        total_price: Total price before tax.
        tax_rate: Tax rate as a decimal (e.g. 0.08 for 8%).

    Returns:
        Tax amount rounded to 2 decimal places.
    """
    return round(total_price * tax_rate, 2)


def enrich_message(
    row: dict[str, Any],
    region_lookup: dict[str, float],
    discount_lookup: dict[str, float],
) -> dict[str, Any]:
    """Add all derived fields to a raw message row.

    Computes total_price, discount_amount, and tax_amount from the raw message fields
    and the region and discount lookup tables.

    As you add more derived fields,
    extend this function to provide them as well.

    Arguments:
        row: A validated raw message row.
        region_lookup: A dict mapping region_id to tax_rate_pct.
        discount_lookup: A dict mapping discount_code to discount_pct.

    Returns:
        A new dict containing all original fields plus derived fields.
    """
    quantity = int(row.get("quantity", 0))
    unit_price = float(row.get("unit_price", 0.0))
    region_id = str(row.get("region_id", ""))
    discount_code = str(row.get("discount_code", ""))

    tax_rate = get_tax_rate(region_id, region_lookup)
    total_price = compute_total_price(quantity, unit_price)
    discount_rate = get_discount_rate(discount_code, discount_lookup)
    discount_amount = compute_discount_amount(total_price, discount_rate)

    price_after_discount = round(total_price - discount_amount, 2)
    tax_amount = compute_tax_amount(price_after_discount, tax_rate)

    total = round(price_after_discount + tax_amount, 2)
    return {
        **row,
        "subtotal": total_price,
        "discount_amount": discount_amount,
        "price_after_discount": price_after_discount,
        "tax_amount": tax_amount,
        "total": total,
    }


def get_discount_rate(discount_code: str, discount_lookup: dict[str, float]) -> float:
    """Look up the discount rate for a code.

    The discount rate is stored as a percentage in discount_codes.csv (e.g. 10.0 for 10%).
    This function converts it to a decimal for use in calculations.

    Arguments:
        discount_code: The discount code from the message (e.g. "WELCOME10").
        discount_lookup: A dict mapping discount_code to discount_pct (as a float).

    Returns:
        The discount rate as a decimal (e.g. 0.10 for 10%), or 0.0 if code not found.
    """
    if not discount_code or discount_code not in discount_lookup:
        return 0.0

    return discount_lookup[discount_code] / 100.0


def get_tax_rate(region_id: str, region_lookup: dict[str, float]) -> float:
    """Look up the tax rate for a region.

    The tax rate is stored as a percentage in regions.csv (e.g. 8.0 for 8%).
    This function converts it to a decimal for use in calculations.

    Arguments:
        region_id: The region identifier from the message (e.g. "US-MO").
        region_lookup: A dict mapping region_id to tax_rate_pct (as a float).

    Returns:
        The tax rate as a decimal (e.g. 0.08 for 8%).
    """
    if region_id in region_lookup:
        return region_lookup[region_id] / 100.0

    LOG.warning(
        f"Region {region_id!r} not in lookup table. "
        f"Using default tax rate {TAX_RATE_DEFAULT}."
    )

    return TAX_RATE_DEFAULT
