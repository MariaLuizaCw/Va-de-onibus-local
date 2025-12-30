// Purpose: centralizar estado e ações da aplicação (autenticação + busca de rotas).
import { get, writable } from 'svelte/store';
import type { ApiRecord, LoginCredentials } from '$lib/types/api';
import {
    login as loginService,
    fetchRouteData,
    UnauthorizedError,
    TOKEN_STORAGE_KEY,
    cities as cityOptions
} from '$lib/services/api';

const canUseBrowser = typeof window !== 'undefined';

type AppState = {
    city: string;
    linha: string;
    tableData: ApiRecord[];
    loading: boolean;
    statusMessage: string;
    errorMessage: string | null;
    lastFetched: string | null;
    authToken: string | null;
    loginLoading: boolean;
    loginError: string | null;
    preferredSortFields: string[];
};

const initialToken = canUseBrowser ? localStorage.getItem(TOKEN_STORAGE_KEY) : null;

const initialState: AppState = {
    city: 'rio',
    linha: '',
    tableData: [],
    loading: false,
    statusMessage: 'Selecione a cidade e informe a linha para ver os registros.',
    errorMessage: null,
    lastFetched: null,
    authToken: initialToken,
    loginLoading: false,
    loginError: null,
    preferredSortFields: ['datahora']
};

function createAppStore() {
    const store = writable<AppState>(initialState);

    function setCity(value: string) {
        store.update((s) => ({
            ...s,
            city: value,
            preferredSortFields: value === 'rio' ? ['datahora'] : ['event_date', 'EventDate'],
            statusMessage: 'Informe a linha desejada para iniciar a busca.',
            tableData: []
        }));
    }

    function setLine(value: string) {
        store.update((s) => ({ ...s, linha: value }));
    }

    async function login(credentials: LoginCredentials) {
        store.update((s) => ({ ...s, loginLoading: true, loginError: null }));
        try {
            const { token } = await loginService(credentials);
            if (canUseBrowser) {
                localStorage.setItem(TOKEN_STORAGE_KEY, token);
            }
            store.update((s) => ({
                ...s,
                authToken: token,
                loginError: null,
                statusMessage: 'Token validado. Faça uma busca para continuar.',
                errorMessage: null
            }));
        } catch (error) {
            const message = error instanceof Error ? error.message : 'Não foi possível conectar ao backend.';
            store.update((s) => ({ ...s, loginError: message }));
        } finally {
            store.update((s) => ({ ...s, loginLoading: false }));
        }
    }

    function logout(message?: string) {
        if (canUseBrowser) {
            localStorage.removeItem(TOKEN_STORAGE_KEY);
        }
        store.update((s) => ({
            ...s,
            authToken: null,
            linha: '',
            tableData: [],
            lastFetched: null,
            statusMessage: message ?? 'Sessão encerrada.',
            errorMessage: message ? message : null,
            loginError: null
        }));
    }

    async function fetchRoute() {
        const state = get(store);
        const trimmedLine = state.linha.trim();

        if (!trimmedLine) {
            store.update((s) => ({
                ...s,
                tableData: [],
                errorMessage: 'Informe uma linha para continuar.',
                statusMessage: 'Linha obrigatória.'
            }));
            return;
        }

        if (!state.authToken) {
            store.update((s) => ({
                ...s,
                loading: false,
                errorMessage: 'Faça login para consultar dados.',
                statusMessage: 'Token JWT necessário.'
            }));
            return;
        }

        store.update((s) => ({
            ...s,
            loading: true,
            errorMessage: null,
            statusMessage: 'Carregando registros...'
        }));

        try {
            const records = await fetchRouteData(state.city, trimmedLine, state.authToken);
            const timestamp = new Date().toLocaleTimeString();
            store.update((s) => ({
                ...s,
                tableData: records,
                lastFetched: timestamp,
                statusMessage:
                    records.length === 0
                        ? 'Nenhum registro encontrado para essa linha.'
                        : `Foram retornados ${records.length} registros às ${timestamp}.`,
                errorMessage: null
            }));
        } catch (error) {
            if (error instanceof UnauthorizedError) {
                logout(error.message);
                return;
            }
            const message = error instanceof Error ? error.message : 'Não foi possível conectar ao backend.';
            store.update((s) => ({
                ...s,
                tableData: [],
                errorMessage: message,
                statusMessage: 'Tente novamente mais tarde.'
            }));
        } finally {
            store.update((s) => ({ ...s, loading: false }));
        }
    }

    return {
        subscribe: store.subscribe,
        setCity,
        setLine,
        login,
        logout,
        fetchRoute
    };
}

export const appStore = createAppStore();
export const cities = cityOptions;
