<script lang="ts">
	import type { LayoutProps } from './$types';

	import '../app.css';
	import Footer from './Footer.svelte';
	import Header from './Header.svelte';

	let { data, children }: LayoutProps = $props();
</script>

<div class="mx-auto flex min-h-screen flex-col bg-violet-200">
	<Header />

	<main class="flex flex-1 flex-col px-4 py-1">
		{@render children()}
	</main>

	<Footer statusData={data.healthz} />
</div>
