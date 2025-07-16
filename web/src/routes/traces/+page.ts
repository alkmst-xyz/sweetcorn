import { API_BASE, type TraceRecord } from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch }) => {
	const res = await fetch(`${API_BASE}/api/v1/traces`);
	return { traces: (await res.json()) as Array<TraceRecord> };
};
