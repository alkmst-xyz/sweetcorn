#syntax=docker/dockerfile:1.7.0-labs

ARG NODE_VERSION=22.17.0-alpine

FROM node:${NODE_VERSION} AS build-ui

ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable

WORKDIR /workspace

COPY web/package.json web/pnpm-lock.yaml web/.npmrc ./

RUN --mount=type=cache,id=pnpm,target=/pnpm/store \
    pnpm install --frozen-lockfile --ignore-scripts

COPY web/ .
RUN pnpm run build

FROM golang:1.24.1 AS build

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY Makefile ./
COPY --parents **/*.go ./

COPY --from=build-ui /workspace/build ./web/build

RUN make build

FROM golang:1.24.1

WORKDIR /

COPY --from=build /workspace/build/bin/sweetcorn /bin/sweetcorn

EXPOSE 4317
EXPOSE 4318
EXPOSE 13579

CMD [ "bin/sweetcorn" ]
