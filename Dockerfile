#syntax=docker/dockerfile:1.7.0-labs

FROM node:22.17.0-alpine AS build-ui

ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable

WORKDIR /workspace

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./ 

RUN --mount=type=cache,id=pnpm,target=/pnpm/store \
    --mount=type=bind,source=packages/sweetcorn-ui/package.json,target=packages/sweetcorn-ui/package.json \
    --mount=type=bind,source=packages/sweetcorn-ui/pnpm-lock.yaml,target=packages/sweetcorn-ui/pnpm-lock.yaml \
    pnpm install --frozen-lockfile --ignore-scripts

COPY packages/ ./packages
RUN pnpm --filter sweetcorn-ui run build

FROM golang:1.25.5 AS build

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY --parents **/*.go ./

COPY --from=build-ui /workspace/packages/sweetcorn-ui/build ./internal/app/build

RUN go build -o sweetcorn .

FROM golang:1.25.5

WORKDIR /

COPY --from=build /workspace/sweetcorn /bin/sweetcorn

EXPOSE 4317
EXPOSE 4318
EXPOSE 13579

CMD [ "bin/sweetcorn" ]
