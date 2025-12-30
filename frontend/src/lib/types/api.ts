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
