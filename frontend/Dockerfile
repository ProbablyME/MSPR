# ── Stage 1 : Build React/Vite ────────────────────────────────────────────
FROM node:20-alpine AS build

WORKDIR /app

# Copie des fichiers de dépendances en premier pour exploiter le cache Docker
COPY package.json ./
RUN npm install --frozen-lockfile

# Copie du reste du code source
COPY . .

# Jeton API injecté au build (Vite remplace import.meta.env.VITE_API_TOKEN
# à la compilation). Fourni par docker-compose via build.args.
ARG VITE_API_TOKEN
ENV VITE_API_TOKEN=$VITE_API_TOKEN

# Build de production (assets minifiés + hachés)
RUN npm run build

# ── Stage 2 : Serve avec nginx ────────────────────────────────────────────
FROM nginx:1.27-alpine AS serve

# Supprime la conf nginx par défaut
RUN rm /etc/nginx/conf.d/default.conf

# Copie notre configuration avec proxy vers l'API
COPY nginx.conf /etc/nginx/conf.d/obrail.conf

# Copie du build Vite
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

# Healthcheck : vérifie que nginx répond
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
