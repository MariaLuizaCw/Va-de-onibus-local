# Frontend (SvelteKit) Dockerfile
FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY . .

# Ensure SvelteKit sync runs after all sources are present.
RUN npm run prepare

# SvelteKit dev server default port
EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]
