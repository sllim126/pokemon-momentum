---
title: Why I Built a Pokemon Market Screener
date: 2026-03-19
description: Most Pokemon price tools tell you what a card sold for. They don't tell you what it's doing. I built a market screener to track momentum, breadth, and early uptrends across English and Japanese cards and sealed products — so you can find what's moving before it's obvious.
author: Adam Mills
categories: Pokemon Investing, Market Tools, Market Talk
tags: Pokemon Investing, Pokemon Market, Price Momentum, Sealed, Japanese, Singles, Card Market Tracker, Card Prices
cover: poke6s-market-tools-04e8e5e1ac
old_path: /resources/why-i-built-a-pokemon-market-screener
draft: false
---
Pokemon pricing data exists everywhere. The problem isn't access — it's interpretation. I wanted something that could tell me what was actually moving, not just what something last sold for.

01 — The Problem

## Prices Are Easy. Signals Are Hard.

If you've spent time in the Pokemon collecting or investing space, you already know the pricing tools. You can look up what a card sold for last week, or check a set's current price guide. That data is out there.

But knowing a price and knowing what a price is *doing* are two different things. Most collectors and buyers can see prices — they can't easily see momentum, breadth, or early movement. By the time something looks obviously hot, it usually already is.

The screener isn't a price list. It's a way to make the market more interpretable — to surface what's actually moving before everyone else has already noticed it.

Sorting through raw price guides, spreadsheets, and individual set pages one at a time is slow. Big movers are easy to spot in hindsight. I wanted something that could show early signals at scale, across the whole market, every day.

◈ 02 — What It Does

## A Market Research Tool, Not a Price List

The screener tracks cards and sealed products across the Pokemon market — both English and Japanese — and surfaces different types of signals depending on what you're looking for.

- → Top movers and breakouts across singles and sealed
- → Early uptrends — cards that are starting to move before they become obvious
- → Pullback opportunities on cards with sustained upward momentum
- → Set-level breadth signals showing how broadly a set is participating
- → Browse by product, set, and market — EN and JP side by side

The goal is to make it possible to compare individual card behavior with broader set-level activity, so you're not just looking at one data point in isolation.

◈ 03 — How It Thinks

## Beyond Raw Prices

The screener isn't showing you raw prices. It's using trend and momentum concepts — applied to Pokemon card data specifically.

30-Day Δ

Recent price change over the last 30 days — not just last sale

Moving Avgs

Smoothed trends that filter out noise from one-off sales

Early Uptrend

Cards beginning to move before the momentum is obvious

Recency

How recently activity happened — stale prices get flagged

Breadth

How broadly a set is participating, not just top cards

The goal is to distinguish real, sustained movement from one-off spikes or stale prices that haven't updated in weeks. Both will mislead you if you don't account for them.

◈ 04 — Breadth

## One Chase Card Doesn't Make a Strong Set

A set can look expensive and active because it has one or two high-value chase cards — even when the rest of the set is completely flat.

Broad-Based Set — 78% of cards movingHealthy Top-Heavy Set — movement in 22% of cardsNarrow Mid-Range Set — steady participation at 65%Moderate  

Illustrative example — breadth score as % of set cards showing upward momentum

Breadth measures how much of a set is participating in a move. A truly healthy set has broad-based activity. A top-heavy set is being carried by a few cards — and that's a meaningful difference when you're deciding what to buy or how much confidence to have in a set's overall trajectory.

"The goal is to tell the difference between a set that's genuinely moving and one that just has an expensive card in it."

◈ 05 — The Payoff

## A Repeatable Way to Find What's Worth Looking At

Before building this, I was doing the same thing a lot of collectors do: checking set pages, skimming price guides, and occasionally noticing something had moved after it already moved. It worked, but it was slow and inconsistent.

The screener gives me a repeatable process. Every day I can see what's actually moving, what's in an early uptrend, and what's worth researching more deeply. It helps me decide what to buy, what to watch, and what to pass on — without starting from scratch each time.

I think it can help other collectors and buyers for the same reasons. Not everyone wants to dig into pricing spreadsheets. Having something that surfaces movement clearly — across both singles and sealed, across EN and JP — makes the market more accessible.

◈ 06 — Under the Hood

## What's Actually Running It

The screener is built on daily pricing data, card metadata, and calculated indicators that refresh on a regular cadence. It covers both the English and Japanese Pokemon markets, including singles and sealed products.

The indicators are calculated fresh rather than cached indefinitely — stale signals are worse than no signals. It's still being refined as I get more data and figure out which signals are most predictive versus which ones just look interesting.

◈ 07 — What's Next

## The Roadmap

A few things I'm actively working on or thinking about:

📈 Better set-level charts and historical trend views ⭐ Favorites and personal watchlists 🎯 More refined "good buy" signal scoring 📱 Mobile UX improvements 💎 Master set cost tracking and value concentration metrics  

The longer-term goal is to make the set-level view as useful as the individual card view — so you can evaluate a whole set's health in the same way you'd evaluate a single card's momentum.

## It's a Research Tool, Not a Price List

The whole point is to make the Pokemon market easier to interpret and more actionable — whether you're a collector, an investor, or just someone who wants to know what's worth paying attention to right now.

[Explore the Screener](https://market.poke6s.com)
