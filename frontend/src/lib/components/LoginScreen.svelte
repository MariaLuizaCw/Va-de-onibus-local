<script lang="ts">
    import { createEventDispatcher } from 'svelte';
    import AuthPanel from './AuthPanel.svelte';

    export let loginLoading = false;
    export let loginError: string | null = null;

    const dispatch = createEventDispatcher<{ login: { username: string; password: string } }>();

    function handleLogin(event: CustomEvent<{ username: string; password: string }>) {
        dispatch('login', event.detail);
    }
</script>

<section class="login-screen">
    <div class="login-copy">
        <p class="eyebrow">Dashboard Vá de Ônibus</p>
        <h1>Acesso restrito</h1>
        <p>
            Entre com suas credenciais para visualizar os dados da plataforma.
        </p>
    </div>

    <AuthPanel
        authToken={null}
        {loginLoading}
        {loginError}
        on:login={handleLogin}
    />
</section>

<style>
    .login-screen {
        min-height: 100vh;
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: clamp(2rem, 4vw, 3rem);
        align-items: center;
        padding: 3rem clamp(1.5rem, 3vw, 4rem);
    }

    .login-copy {
        max-width: 420px;
    }

    .login-copy h1 {
        margin: 0 0 1rem;
        font-size: clamp(2rem, 4vw, 2.8rem);
    }

    .login-copy p {
        margin: 0;
        color: rgba(226, 232, 240, 0.8);
        line-height: 1.6;
    }

    .eyebrow {
        letter-spacing: 0.3em;
        text-transform: uppercase;
        font-size: 0.75rem;
        color: #a5b4fc;
        margin: 0 0 0.75rem;
    }

    @media (max-width: 768px) {
        .login-screen {
            grid-template-columns: 1fr;
        }
    }
</style>
