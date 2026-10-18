FROM nginx:1.27-alpine

COPY nginx/default.conf /etc/nginx/conf.d/default.conf
COPY nginx/security-headers.conf /etc/nginx/snippets/security-headers.conf
COPY index.html my_card_planner.html robots.txt /usr/share/nginx/html/
# .dockerignore limits this to the image files the planner actually loads.
COPY cards/ /usr/share/nginx/html/cards/
