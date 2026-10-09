"""Selling-cost assumptions for the hold check. Edit the numbers here; nothing else
needs to change.

Defaults marked ESTIMATE are not confirmed against an invoice yet:
- Marketplace fees are the commonly published TCGplayer rate (commission plus payment
  processing). One real order (a $5.20 sale that netted $4.15) suggests TCGplayer's
  effective take may be slightly higher; check the Seller Portal fee page and adjust.
- Squarespace is card processing only; add your plan's transaction fee if it has one.
- Shipping tiers follow how the store ships: plain envelope under $35, tracked from
  $35, tracked with signature from $50.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Channel:
    label: str
    fee_pct: float  # fraction of the sale price
    fee_fixed: float  # dollars per order


CHANNELS = {
    "tcgplayer": Channel("TCGplayer", fee_pct=0.1325, fee_fixed=0.30),  # ESTIMATE
    "ebay": Channel("eBay", fee_pct=0.1325, fee_fixed=0.30),  # ESTIMATE: same as TCGplayer per owner
    "squarespace": Channel("Squarespace", fee_pct=0.029, fee_fixed=0.30),  # ESTIMATE: processing only
}
DEFAULT_CHANNEL = "tcgplayer"

# (minimum sale price, shipping + supplies cost, description), checked from the top.
SHIPPING_TIERS = [
    (50.00, 8.50, "tracked with signature"),
    (35.00, 5.00, "tracked"),  # ESTIMATE
    (0.00, 1.25, "plain envelope"),  # ESTIMATE: stamp, envelope, toploader, sleeve
]


def shipping_cost(sale_price: float) -> tuple[float, str]:
    for minimum, cost, description in SHIPPING_TIERS:
        if sale_price >= minimum:
            return cost, description
    return SHIPPING_TIERS[-1][1], SHIPPING_TIERS[-1][2]


def net_proceeds(sale_price: float, channel: Channel) -> float:
    """What the seller keeps from one sale after marketplace fees and shipping."""
    ship, _ = shipping_cost(sale_price)
    return sale_price - (sale_price * channel.fee_pct + channel.fee_fixed) - ship


def break_even_price(buy_price: float, channel: Channel) -> float:
    """Lowest sale price that returns the buy price after fees and shipping.

    Shipping steps up at tier boundaries, so this searches each tier instead of
    inverting one formula.
    """
    candidates = []
    upper = float("inf")
    for minimum, cost, _ in SHIPPING_TIERS:
        # Within this tier net proceeds rise with price, so the answer is the formula
        # result clamped to the tier's floor, provided it stays below the next tier.
        price = max(minimum, (buy_price + channel.fee_fixed + cost) / (1 - channel.fee_pct))
        if price < upper:
            candidates.append(price)
        upper = minimum
    return min(candidates)
