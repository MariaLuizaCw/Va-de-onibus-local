// Purpose: unifica chamadas HTTP ao backend, incluindo login e consulta de rotas.
import { PUBLIC_BACKEND_URL } from '$env/static/public';
import type { ApiRecord, LoginCredentials, RawResponse, AuthResponse, CityOption, StatsResponse, JobStatsResponse, JobTimelineEntry, JobsConfig } from '$lib/types/api';
import jobsConfig from '$lib/common_settings/jobs.json';


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
    { id: 'angra', label: 'Angra dos Reis' },
    { id: 'rioita', label: 'RioIta' }
];

function normalizeRecords(payload: RawResponse, targetKey: string): ApiRecord[] {
    if (Array.isArray(payload)) {
        return payload;
    }

    if ('ordens' in payload && Array.isArray(payload.ordens)) {
        return payload.ordens;
    }

    if ('positions' in payload && Array.isArray(payload.positions)) {
        return payload.positions;
    }

    // Se for um objeto com muitas chaves (RioIta sem busca específica)
    const map = payload as Record<string, ApiRecord[]>;
    
    // Verificar se é uma resposta do RioIta (objeto com muitas chaves numéricas)
    const keys = Object.keys(map);
    if (keys.length > 1 && keys.every(key => !isNaN(Number(key)))) {
        // É uma resposta do RioIta com todas as ordens
        const allRecords: ApiRecord[] = [];
        for (const key of keys) {
            const records = map[key];
            if (Array.isArray(records)) {
                allRecords.push(...records);
            }
        }
        return allRecords;
    }

    // Caso normal: buscar pela chave específica
    const records = map[targetKey];
    if (Array.isArray(records)) {
        return records;
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

export async function fetchRouteData(city: string, searchValue: string, token: string): Promise<ApiRecord[]> {
    // RioIta usa 'ordem' como parâmetro de busca, outros usam 'linha'
    const bodyParam = city === 'rioita' 
        ? { ordem: searchValue }
        : { linha: searchValue };

    const response = await fetch(`${BACKEND_BASE_URL}/${city}_onibus`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${token}`
        },
        body: JSON.stringify(bodyParam)
    });

    if (response.status === 401) {
        throw new UnauthorizedError();
    }

    if (!response.ok) {
        const message = await buildErrorMessage(response);
        throw new Error(message);
    }

    const payload: RawResponse = await response.json();
    return normalizeRecords(payload, searchValue);
}

export async function fetchStats(token: string): Promise<StatsResponse> {
    const response = await fetch(`${BACKEND_BASE_URL}/stats/lines`, {
        method: 'GET',
        headers: {
            Authorization: `Bearer ${token}`
        }
    });

    if (response.status === 401) {
        throw new UnauthorizedError();
    }

    if (!response.ok) {
        const message = await buildErrorMessage(response);
        throw new Error(message);
    }

    return response.json();
}

// Job Stats API
export async function fetchJobStats(token: string, date?: string): Promise<JobStatsResponse> {
    const url = date 
        ? `${BACKEND_BASE_URL}/jobs/stats?date=${date}`
        : `${BACKEND_BASE_URL}/jobs/stats`;
    
    const response = await fetch(url, {
        method: 'GET',
        headers: {
            Authorization: `Bearer ${token}`
        }
    });

    if (response.status === 401) {
        throw new UnauthorizedError();
    }

    if (!response.ok) {
        const message = await buildErrorMessage(response);
        throw new Error(message);
    }

    return response.json();
}

export async function fetchJobTimeline(token: string, jobName: string, date?: string, includeChildren = false): Promise<JobTimelineEntry[]> {
    const params = new URLSearchParams();
    if (date) params.set('date', date);
    if (includeChildren) params.set('includeChildren', 'true');
    
    const url = `${BACKEND_BASE_URL}/jobs/timeline/${encodeURIComponent(jobName)}?${params}`;
    
    const response = await fetch(url, {
        method: 'GET',
        headers: {
            Authorization: `Bearer ${token}`
        }
    });

    if (response.status === 401) {
        throw new UnauthorizedError();
    }

    if (!response.ok) {
        const message = await buildErrorMessage(response);
        throw new Error(message);
    }

    return response.json();
}


export async function fetchJobHourlyDistribution(token: string, jobName: string, date?: string): Promise<JobHourlyDistribution[]> {
    const url = date 
        ? `${BACKEND_BASE_URL}/jobs/hourly/${encodeURIComponent(jobName)}?date=${date}`
        : `${BACKEND_BASE_URL}/jobs/hourly/${encodeURIComponent(jobName)}`;
    
    const response = await fetch(url, {
        method: 'GET',
        headers: {
            Authorization: `Bearer ${token}`
        }
    });

    if (response.status === 401) {
        throw new UnauthorizedError();
    }

    if (!response.ok) {
        const message = await buildErrorMessage(response);
        throw new Error(message);
    }

    return response.json();
}

export async function fetchJobsConfig(): Promise<JobsConfig> {
    // Retorna o arquivo importado diretamente, sem requisição HTTP
    return jobsConfig as JobsConfig;
}

export { cities };
