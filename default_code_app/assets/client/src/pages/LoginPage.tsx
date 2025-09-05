import { useAppContext } from '@/contexts';
import { useLoadingState } from '@/hooks';
import { Button, Stack, TextField } from '@mui/material';
import { useInsight } from '@semoss/sdk-react';
import { ChangeEvent, useRef, useState } from 'react';
import { Navigate, useLocation } from 'react-router';

/**
 * Renders a the login page if the user is not already logged in, otherwise sends them to the home page.
 *
 * @component
 */
export const LoginPage = () => {
    const { isAuthorized } = useInsight();
    const { login } = useAppContext();
    const { state } = useLocation(); // If the user was routed here, then there may be information about where they were trying to go

    // If the user is already authorized, we can route them off of this page. If the user was routed here, attempt to send them back to their target
    if (isAuthorized) return <Navigate to={state?.target ?? '/'} />;

    /**
     * State / Refs
     */
    const [username, setUsername] = useState<string>('');
    const [password, setPassword] = useState<string>('');
    const [isLoginLoading, setIsLoginLoading] = useLoadingState(false);
    const [showError, setShowError] = useState<boolean>(false); // State to show the user that there was an error with their login
    const passwordInputRef = useRef<HTMLInputElement>(null); // A ref to store the password input, so that pressing Enter in the username box will focus it

    /**
     * Functions
     */
    const passwordLogin = async () => {
        const loadingKey = setIsLoginLoading(true);

        // Attempt to log in
        const success = await login(username, password);
        if (!success) {
            setShowError(true);
            passwordInputRef.current?.focus();
        }
        setIsLoginLoading(false, loadingKey);
    };

    // When the user begins typing, clear out the errors
    const updateState = (
        state: 'username' | 'password',
        event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
    ) => {
        setShowError(false);
        (state === 'username' ? setUsername : setPassword)(event.target.value);
    };

    /**
     * Constants
     */
    const isLoginReady = username && password && !showError;

    return (
        <Stack spacing={2}>
            <TextField
                label="Username"
                value={username}
                onChange={(event) => updateState('username', event)}
                error={showError}
                required
                disabled={isLoginLoading}
                onKeyDown={(event) => {
                    if (event.key === 'Enter' && username) {
                        // If the user hits enter, take them to the password box
                        passwordInputRef.current?.focus();
                    }
                }}
            />
            <TextField
                label="Password"
                value={password}
                onChange={(event) => updateState('password', event)}
                error={showError}
                required
                helperText={
                    showError ? 'Username and password do not match' : ' '
                }
                disabled={isLoginLoading}
                type="password"
                onKeyDown={(event) => {
                    if (event.key === 'Enter' && isLoginReady) {
                        // If the user hits Enter, have them attempt to log in
                        passwordLogin();
                    }
                }}
                inputRef={passwordInputRef}
            />
            <Button
                variant="contained"
                onClick={passwordLogin}
                disabled={!isLoginReady}
                loading={isLoginLoading}
            >
                Log in
            </Button>
        </Stack>
    );
};
