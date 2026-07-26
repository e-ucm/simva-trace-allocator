# ---------------------------------------
# Base
# ---------------------------------------
FROM timbru31/node-alpine-git:22 AS base

WORKDIR /app

# Common dependencies
RUN apk add --no-cache curl ca-certificates bash

FROM base AS deps

COPY --chown=node:node package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci

FROM deps AS dev

RUN npm install -g clinic
RUN chown -R node:node /app/node_modules

USER node
EXPOSE 3050

# Default CMD, can be overridden by docker-compose
CMD [ "npm", "run", "dev" ]

FROM node:22-alpine AS prod

WORKDIR /app

COPY --chown=node:node --from=deps /app/node_modules ./node_modules
COPY --chown=node:node ./src ./src
COPY --chown=node:node ./jsconfig.json ./jsconfig.json

USER node

# Make port 3050 available to the world outside this container
EXPOSE 3050

CMD [ "npm", "start" ]