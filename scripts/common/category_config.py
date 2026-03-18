from dataclasses import dataclass


@dataclass(frozen=True)
class CategoryConfig:
    """Central naming contract for per-category files, tables, and labels."""
    category_id: int
    slug: str
    label: str

    @property
    def groups_csv(self) -> str:
        return f"{self.slug}_groups.csv"

    @property
    def products_csv(self) -> str:
        return f"{self.slug}_products.csv"

    @property
    def prices_csv(self) -> str:
        return f"{self.slug}_prices_all_days.csv"

    @property
    def prices_named_csv(self) -> str:
        return f"{self.slug}_prices_named.csv"

    @property
    def groups_table(self) -> str:
        return f"{self.slug}_groups"

    @property
    def products_table(self) -> str:
        return f"{self.slug}_products"

    @property
    def prices_named_table(self) -> str:
        return f"{self.slug}_prices_named"

    @property
    def product_signal_table(self) -> str:
        return f"{self.slug}_product_signal_snapshot"

    @property
    def product_signal_csv(self) -> str:
        return f"{self.slug}_product_signal_snapshot.csv"

    @property
    def group_signal_table(self) -> str:
        return f"{self.slug}_group_signal_snapshot"

    @property
    def group_signal_csv(self) -> str:
        return f"{self.slug}_group_signal_snapshot.csv"


def get_category_config(category_id: int) -> CategoryConfig:
    """Resolve known categories to friendly names and stable storage prefixes."""
    if category_id == 3:
        return CategoryConfig(category_id=3, slug="pokemon", label="Pokemon")
    if category_id == 85:
        return CategoryConfig(category_id=85, slug="pokemon_jp", label="Pokemon Japanese")
    return CategoryConfig(category_id=category_id, slug=f"category_{category_id}", label=f"Category {category_id}")
