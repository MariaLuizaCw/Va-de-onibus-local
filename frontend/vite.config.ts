import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [sveltekit()],
	server: {
		host: '0.0.0.0',
		port: 5173,
		allowedHosts: [
			'galeao.cos.ufrj.br',
			'localhost',
			'127.0.0.1'
		]
	}
});