// Purpose: unifica chamadas HTTP ao backend, incluindo login e consulta de rotas.
import { PUBLIC_BACKEND_URL } from '$env/static/public';
import type { ApiRecord, LoginCredentials, RawResponse, AuthResponse, CityOption } from '$lib/types/api';

const BACKEND_BASE_URL = PUBLIC_BACKEND_URL;
export const TOKEN_STORAGE_KEY = 'vadeonibus_jwt';

export class UnauthorizedError extends Error {
    constructor(message = 'Token inválido ou expirado.') {
        super(message);
        this.name = 'UnauthorizedError';
    }
}

const cities: CityOption[] = [
    { id: 'rio', label: 'Rio' },
    { id: 'angra', label: 'Angra dos Reis' }
];

function normalizeRecords(payload: RawResponse, targetLine: string): ApiRecord[] {
    if (Array.isArray(payload)) {
        return payload;
    }

    if ('ordens' in payload && Array.isArray(payload.ordens)) {
        return payload.ordens;
    }

    const map = payload as Record<string, ApiRecord[]>;
    const lineRecords = map[targetLine];
    if (Array.isArray(lineRecords)) {
        return lineRecords;
    }
    return [];
}

const buildErrorMessage = async (response: Response) => {
    const payload = await response.json().catch(() => null);
    return payload?.error ?? `Falha ao buscar: ${response.status}`;
};

export async function login(credentials: LoginCredentials): Promise<AuthResponse> {
    const response = await fetch(`${BACKEND_BASE_URL}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(credentials)
    });

    if (!response.ok) {
        const message = await buildErrorMessage(response);
        throw new Error(message);
    }

    return response.json();
}

export async function fetchRouteData(city: string, linha: string, token: string): Promise<ApiRecord[]> {
    const response = await fetch(`${BACKEND_BASE_URL}/${city}_onibus`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`
        },
        body: JSON.stringify({ linha })
    });

    if (response.status === 401) {
        throw new UnauthorizedError();
    }

    if (!response.ok) {
        const message = await buildErrorMessage(response);
        throw new Error(message);
    }

    const payload: RawResponse = await response.json();
    return normalizeRecords(payload, linha);
}

export { cities };
