import { API_BASE, type LogRecord } from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch }) => {
	const res = await fetch(`${API_BASE}/api/v1/logs`);
	return { logs: (await res.json()) as Array<LogRecord> };
};
