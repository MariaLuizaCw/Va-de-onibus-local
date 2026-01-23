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

// Job Stats Types
export type JobChildStats = {
    jobName: string;
    parentJob: string;
    executionCount: number;
    avgDurationMs: number;
    stddevDurationMs: number;
    minDurationMs: number;
    maxDurationMs: number;
    successCount: number;
    errorCount: number;
    status: 'success' | 'error' | 'mixed';
};

export type JobParentStats = {
    jobName: string;
    executionCount: number;
    avgDurationMs: number;
    stddevDurationMs: number;
    minDurationMs: number;
    maxDurationMs: number;
    successCount: number;
    errorCount: number;
    status: 'success' | 'error' | 'mixed';
    children: JobChildStats[];
};

export type JobStatsResponse = {
    date: string;
    jobs: JobParentStats[];
};

export type JobTimelineEntry = {
    jobName: string;
    parentJob: string | null;
    subtask: boolean;
    startedAt: string;
    finishedAt: string;
    durationMs: number;
    status: 'success' | 'error';
    infoMessage: string | null;
    errorMessage: string | null;
};

export type JobHourlyDistribution = {
    hour: number;
    total: number;
    successCount: number;
    errorCount: number;
    avgDurationMs: number;
};

export type JobConfig = {
    name: string;
    description: string;
    cron: string;
    runOnStartup: boolean;
};

export type JobsConfig = {
    jobs: JobConfig[];
};
