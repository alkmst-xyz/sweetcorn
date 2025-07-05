<script lang="ts">
	import Footer from './Footer.svelte';
	import Header from './Header.svelte';
	import '../app.css';

	let { children } = $props();
</script>

<div class="mx-auto flex min-h-screen max-w-4xl flex-col bg-violet-200">
	<Header />

	<main class="flex flex-1 flex-col px-4 py-1">
		{@render children()}
	</main>

	<Footer />
</div>
