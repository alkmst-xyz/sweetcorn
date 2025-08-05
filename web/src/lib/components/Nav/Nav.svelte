<script lang="ts">
	import { page } from '$app/state';
	import NavItem from './NavItem.svelte';
	import type { NavItemType } from './types';

	let { items }: { items: Array<NavItemType> } = $props();
</script>

<nav class="flex w-full">
	<ul class="flex space-x-2">
		{#each items as item}
			{#if item.exact === false}
				<NavItem
					{item}
					ariaCurrent={page.url.hash.startsWith(item.path) ? 'page' : undefined}
				/>
			{:else}
				<NavItem
					{item}
					ariaCurrent={page.url.hash === item.path ? 'page' : undefined}
				/>
			{/if}
		{/each}
	</ul>
</nav>
