import type { FetchType } from './types';
import { API_BASE } from './utils';

export type StatusResponse = {
	status: string;
};

export async function getHealthz(fetch: FetchType) {
	const res = await fetch(`${API_BASE}api/v1/healthz`);
	return (await res.json()) as StatusResponse;
}
