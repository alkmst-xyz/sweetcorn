import { getHealthz } from '$lib/api';
import type { LayoutLoad } from './$types';

export const load: LayoutLoad = async () => {
	return getHealthz();
};
