import { getLogs } from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async () => {
	return getLogs();
};
