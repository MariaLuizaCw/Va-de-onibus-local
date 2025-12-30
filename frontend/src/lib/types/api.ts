// Purpose: centralize API domain types shared across services and stores.
export type ApiRecord = {
    ordem?: string;
    linha?: string;
    latitude?: number | string;
    longitude?: number | string;
    velocidade?: number | string;
    datahora?: number;
    sentido?: string | null;
    datahoraservidor?: number;
} & Record<string, unknown>;

export type RawResponse = ApiRecord[] | { linha: string; ordens: ApiRecord[] } | Record<string, ApiRecord[]>;

export type LoginCredentials = {
    username: string;
    password: string;
};

export type AuthResponse = {
    token: string;
};

export type CityOption = {
    id: string;
    label: string;
};
