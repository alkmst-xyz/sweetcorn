const API_BASE_DEV = 'http://localhost:13579/';
const API_BASE_PROD = '/';

function getBaseUrl(): string {
	// Load API base URL from .env file in development mode.
	if (import.meta.env.DEV) {
		return import.meta.env.VITE_API_BASE_DEV || API_BASE_DEV;
	}

	return API_BASE_PROD;
}

export const API_BASE = getBaseUrl();
