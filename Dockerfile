#syntax=docker/dockerfile:1.7.0-labs

FROM node:22.17.0-alpine AS build-ui

ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable

WORKDIR /workspace

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./ 

RUN --mount=type=cache,id=pnpm,target=/pnpm/store \
    --mount=type=bind,source=src/sweetcorn-ui/package.json,target=src/sweetcorn-ui/package.json \
    --mount=type=bind,source=src/sweetcorn-ui/pnpm-lock.yaml,target=src/sweetcorn-ui/pnpm-lock.yaml \
    pnpm install --frozen-lockfile --ignore-scripts

COPY src/sweetcorn-ui/ ./src/sweetcorn-ui
RUN pnpm --filter sweetcorn-ui run build

FROM golang:1.24.1 AS build

WORKDIR /workspace

COPY go.work go.work.sum ./
COPY --parents ./src/**/go.mod ./
COPY --parents ./src/**/go.sum ./
RUN go work sync

COPY --parents ./src/**/*.go ./

COPY --from=build-ui /workspace/src/sweetcorn-ui/build ./src/sweetcorn/internal/web/build

RUN go build -o sweetcorn ./src/sweetcorn

FROM golang:1.24.1

WORKDIR /

COPY --from=build /workspace/sweetcorn /bin/sweetcorn

EXPOSE 4317
EXPOSE 4318
EXPOSE 13579

CMD [ "bin/sweetcorn" ]
