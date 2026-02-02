# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fintory (핀토리) is a mock stock investment mobile application backend for children. It provides simulated trading on 40 Korean and overseas stocks with real-time price streaming.

## Build Commands

```bash
# Build app-child (main API server)
./gradlew :app-child:clean build

# Build websocket (real-time streaming server)
./gradlew :websocket:clean build

# Run tests
./gradlew test

# Run single module tests
./gradlew :infra:test

# Generate QueryDSL Q-classes (infra module)
./gradlew :infra:compileJava
```

## Running Locally

Requires a `.env` file in project root with:
- `LOCAL_DB_URL`, `LOCAL_DB_USERNAME`, `LOCAL_DB_PASSWORD` (MySQL)
- `JWT_SECRET_KEY`, `ACCESS_TOKEN_EXPIRATION_MINUTES`, `REFRESH_TOKEN_EXPIRATION_DAYS`
- `GOOGLE_CLIENT_ID`, `KAKAO_REST_CLIENT_ID`, `KAKAO_REDIRECT_URI`
- `HANTU_APPKEY`, `HANTU_APPSECRET` (Korean Investment API)
- `DB_APPKEY`, `DB_APPSECRET` (DB Securities API)
- `EOS_API_KEY`, `OPENAI_API_KEY`, `FIREBASE_CONFIG`

Local Redis required on `localhost:6379`.

- **app-child**: runs on port 8080
- **websocket**: runs on port 8081

## Multi-Module Architecture

```
fintory_BE/
├── app-child      # Main REST API application (bootJar enabled)
├── websocket      # Real-time stock streaming via SSE (bootJar enabled)
├── domain         # Domain entities, service interfaces, DTOs
├── infra          # Repository implementations, external API clients
├── auth           # JWT & OAuth2 (Google, Kakao) authentication
└── common         # ApiResponse<T>, DomainException, error codes
```

**Module Dependencies:**
- `app-child` → `auth`, `infra`, `domain`, `common`
- `websocket` → `infra`, `domain`, `common`
- `infra` → `domain`, `common`
- `auth` → `domain`, `common`

## Code Patterns

**Controller Pattern**: Interface + Implementation separation
```
AccountController (interface) → AccountControllerImpl (implementation)
```

**Entity Factory Methods**: Use static factory methods
```java
Account.create(...)
Child.idPwBuilder()...build()
```

**Base Entity**: All JPA entities extend `BaseEntity` providing `createdAt`, `updatedAt` audit fields.

**Real-time Architecture**:
- `websocket` module connects to Korean/Overseas stock WebSocket APIs as a provider
- Clients connect via SSE endpoint at `/api/stock/live-price`
- `StockStreamBridge` bridges WebSocket provider → SSE consumer using Project Reactor (Flux)

**API Response**: All endpoints return `ApiResponse<T>` wrapper from `common` module.

## Key Domain Packages

| Domain | Description |
|--------|-------------|
| `child` | Child user entity (implements `User` interface) |
| `portfolio` | Stock holdings, transactions |
| `stock` | Stock data, Korean & overseas stocks |
| `alarm` | Price alerts, FCM notifications |
| `point` | Point wallet, attendance rewards |

## Testing

Uses JUnit 5 with Mockito. Test classes use:
```java
@ExtendWith(MockitoExtension.class)
@Mock / @InjectMocks
```

## Deployment

GitHub Actions deploys to AWS on push to `dev` branch:
- Docker image → ECR → EC2
- Separate pipelines for `app-child` and `websocket`
