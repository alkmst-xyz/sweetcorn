import {
	getDistinctTraceOperations,
	getDistinctTraceServices,
	getTraces
} from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch }) => {
	return {
		traces: await getTraces(fetch),
		services: await getDistinctTraceServices(fetch),
		operations: await getDistinctTraceOperations(fetch)
	};
};
