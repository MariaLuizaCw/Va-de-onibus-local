<script lang="ts">
    import { createEventDispatcher } from 'svelte';

    export let authToken: string | null = null;
    export let loginLoading = false;
    export let loginError: string | null = null;

    const dispatch = createEventDispatcher<{
        login: { username: string; password: string };
        logout: void;
    }>();

    let username = '';
    let password = '';

    function handleSubmit(event: Event) {
        event.preventDefault();
        dispatch('login', {
            username: username.trim(),
            password,
        });
    }

    function handleLogoutClick() {
        dispatch('logout');
    }
</script>

<div class="auth-panel">
    <div class="auth-header">
        <p class="eyebrow">Autenticação segura</p>
    </div>

    {#if authToken}
        <p class="auth-status success">Você já está logado.</p>
        <button type="button" class="secondary" on:click={handleLogoutClick}>Sair</button>
    {:else}
        <form class="auth-form" on:submit={handleSubmit}>
            <label>
                Usuário
                <input
                    type="text"
                    bind:value={username}
                    autocomplete="username"
                    placeholder="Digite o usuário"
                />
            </label>

            <label>
                Senha
                <input
                    type="password"
                    bind:value={password}
                    autocomplete="current-password"
                    placeholder="••••••••"
                />
            </label>

            {#if loginError}
                <p class="auth-error">{loginError}</p>
            {/if}

            <button class="primary" type="submit" disabled={loginLoading}>
                {#if loginLoading}
                    Autenticando...
                {:else}
                    Conectar
                {/if}
            </button>
        </form>
    {/if}
</div>

<style>
    .auth-panel {
        background: rgba(15, 23, 42, 0.95);
        border-radius: 1.25rem;
        border: 1px solid rgba(59, 130, 246, 0.25);
        padding: 1.5rem;
        display: flex;
        flex-direction: column;
        gap: 1rem;
        box-shadow: 0 20px 35px -20px rgba(2, 6, 23, 0.8);
    }

    .auth-header h3 {
        margin: 0;
        font-size: 1.25rem;
    }

    .auth-form {
        display: flex;
        flex-direction: column;
        gap: 0.85rem;
    }

    .auth-form label {
        font-size: 0.85rem;
        color: rgba(203, 213, 241, 0.8);
        display: flex;
        flex-direction: column;
        gap: 0.35rem;
    }

    .auth-form input {
        border-radius: 0.75rem;
        border: 1px solid rgba(148, 163, 184, 0.4);
        background: rgba(15, 23, 42, 0.4);
        color: #f8fafc;
        padding: 0.85rem 1rem;
    }

    .auth-error {
        color: #f87171;
        margin: 0;
        font-size: 0.85rem;
    }

    .auth-status.success {
        color: #34d399;
        margin: 0;
        font-weight: 500;
    }

    button.secondary {
        border: 1px solid rgba(248, 250, 252, 0.4);
        background: transparent;
        color: #f8fafc;
        border-radius: 999px;
        padding: 0.8rem 1.25rem;
        cursor: pointer;
        transition: transform 0.2s ease, border-color 0.2s ease;
    }

    button.secondary:hover {
        transform: translateY(-1px);
        border-color: rgba(14, 165, 233, 0.8);
    }
</style>
