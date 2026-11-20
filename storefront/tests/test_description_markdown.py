"""Product descriptions: two Squarespace leftovers tidied when shown (app/markdown.py)."""

from storefront.app.markdown import render_description


def test_bold_with_a_space_inside_the_closing_marker_is_fixed():
    html = render_description("- **Card Text: **Each pack contains 10 random cards.")
    assert "<strong>Card Text:</strong> Each pack" in html and "**" not in html


def test_good_bold_next_to_each_other_is_left_alone():
    text = "Look for **Pokémon ex**, **Trainer cards**, and (**Art Rares**) here."
    html = render_description(text)
    assert html.count("<strong>") == 3 and "**" not in html


def test_a_leading_product_details_heading_is_dropped_but_later_ones_stay():
    html = render_description("### **Product Details**\n\n- 10 cards\n\n### Details\n\nMore")
    assert "Product Details" not in html and html.count("<h3>") == 1 and "<li>10 cards</li>" in html


def test_bold_labels_inside_the_text_are_kept():
    html = render_description("Intro.\n\n**Product Details:**\n\n- 1 pack")
    assert "<strong>Product Details:</strong>" in html
