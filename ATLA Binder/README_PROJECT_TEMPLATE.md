# ATLA Binder Planner (Custom Card Set) Project Structure

## 📁 Directory Layout
```
g:/[NEW_PROJECT_NAME]
├── cards/
│   ├── card1.png
│   ├── card2.png
│   └── ... (your custom card images)
├── docker-compose.yml
├── index.html
├── my_card_planner.html
└── README.md
```

## 🛠️ Key Files
### 1. `docker-compose.yml`
```yaml
version: '3'
services:
  custom-card-planner:
    image: nginx:1.27-alpine
    container_name: custom-card-planner
    restart: unless-stopped
    ports:
      - "8090:80"
    volumes:
      - ./:/usr/share/nginx/html:ro
```

### 2. `index.html`
```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="0; url=./my_card_planner.html" />
    <title>Custom Card Planner</title>
  </head>
  <body>
    <p><a href="./my_card_planner.html">Open Custom Card Planner</a></p>
  </body>
</html>
```

### 3. `my_card_planner.html`
> Replace all instances of "ATLA" with your project name. Ensure image paths in JavaScript match `cards/` directory.

## 📝 README.md
```markdown
# Custom Trading Card Planner

## What it does
- Serves a static card planner via Nginx
- Loads cards from `cards/` directory
- Tracks collections in browser `localStorage`

## Deploy
```powershell
docker compose up -d
```
Access at: http://localhost:8090/
```

## Notes
- Replace all card images in `cards/`
- Update HTML title and any hardcoded references
- For persistent storage, add a backend (see Optional Enhancements)
```

## 🔧 Optional Enhancements
1. Add backend (Node.js/Python) for data export/import
2. Add card metadata (rarity, set, etc.) in JSON
3. Implement drag-and-drop sorting
4. Add dark mode/theme switching

## 📌 Cline/Qwen Shorthand
- `{{cards_dir}}` = `g:/[NEW_PROJECT_NAME]/cards/`
- `{{planner_file}}` = `my_card_planner.html`
- `{{port}}` = `8090` (can be changed in docker-compose.yml)
