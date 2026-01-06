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

export type LineStats = {
    linha: string;
    lastUpdate: string | null;
    isActive: boolean;
    totalOrders: number;
    activeOrders: number;
};

export type CityStats = {
    totalLines: number;
    activeLines: number;
    totalOrders: number;
    activeOrders: number;
    lines: LineStats[];
};

export type StatsResponse = {
    rio: CityStats;
    angra: CityStats;
};
