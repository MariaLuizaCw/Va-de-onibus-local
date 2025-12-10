# GPS Data Poller Dashboard

A real-time dashboard application that monitors gps data from the Rio de Janeiro bus system (Data.Rio API).
It consists of a **Node.js/Express backend** for data polling and validaton, and a **SvelteKit frontend** for visualization.

## Features

- **Robust Polling**: Fetches data every minute with a 3-minute overlapping window to ensure no data is missed despite eventual latency.
- **Deduplication**: Filters out duplicate records to ensure data integrity.
- **Real-time Dashboard**: Displays live system status, average latency, active bus lines, and the latest received positions.
- **Persistent Logging**: Stores polling history and errors in `backend/polling_logs.txt`.
- **Premium UI**: Dark-themed, responsive interface using glassmorphism design.

## Prerequisites

- Node.js (LTS version recommended)
- npm

## Quick Start

### 1. Start the Backend
The backend handles the API polling and serves the data endpoints.

```bash
cd backend
npm install
node server.js
```
*Port: 3001*

### 2. Start the Frontend
The frontend provides the visual dashboard.

```bash
cd frontend
npm install
npm run dev -- --open
```
*Local URL: http://localhost:5173*

## Dashboard Metrics

- **Uptime**: Time elapsed since the backend server started.
- **Avg Latency**: The average time difference between the GPS timestamp (bus) and the Server timestamp (Rio API). High values indicate system delays.
- **Active Lines**: Number of unique bus lines detected in the current polling window.
- **Total Polls**: Total number of successful requests made to the external API.

## Project Structure

- `backend/`: Node.js server, `polling_logs.txt`, Dockerfile (optional).
- `frontend/`: SvelteKit application.
