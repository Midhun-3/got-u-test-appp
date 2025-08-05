# Stage 1: Build the TypeScript code
FROM node:18 AS builder

WORKDIR /app

# Copy dependencies first (for better layer caching)
COPY package*.json ./
RUN npm install

# Copy source files and tsconfig
COPY . .

# Build TypeScript
RUN npm run build

# Stage 2: Run the built JS with node
FROM node:18-slim

WORKDIR /app

# Only copy runtime files
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/.env .env
COPY --from=builder /app/package.json ./

# Install only prod dependencies (none in your case, but safe)
RUN npm install --only=production

# Run the app
CMD ["sh", "-c", "env $(cat .env | xargs) node dist/mqtt-redis.js"]
