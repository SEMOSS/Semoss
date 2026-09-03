import { useInsight } from "@semoss/sdk/react";
import { type ChangeEvent, useRef, useState } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useAppContext } from "@/contexts";
import { useLoadingState } from "@/hooks";

/**
 * Renders a the login page if the user is not already logged in, otherwise sends them to the home page.
 *
 * @component
 */
export const LoginPage = () => {
	const { isAuthorized } = useInsight();
	const { login } = useAppContext();
	const { state } = useLocation(); // If the user was routed here, then there may be information about where they were trying to go

	/**
	 * State / Refs
	 */
	const [username, setUsername] = useState<string>("");
	const [password, setPassword] = useState<string>("");
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
		state: "username" | "password",
		event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
	) => {
		setShowError(false);
		(state === "username" ? setUsername : setPassword)(event.target.value);
	};

	/**
	 * Constants
	 */
	const isLoginReady = username && password && !showError;

	// If the user is already authorized, we can route them off of this page. If the user was routed here, attempt to send them back to their target
	if (isAuthorized) return <Navigate to={state?.target ?? "/"} />;

	return (
		<div className="flex flex-col space-y-4">
			<div className="space-y-2">
				<Label htmlFor="username">Username</Label>
				<Input
					id="username"
					value={username}
					onChange={(event) => updateState("username", event)}
					required
					disabled={isLoginLoading}
					onKeyDown={(event) => {
						if (event.key === "Enter" && username) {
							// If the user hits enter, take them to the password box
							passwordInputRef.current?.focus();
						}
					}}
				/>
			</div>
			<div className="space-y-2">
				<Label htmlFor="password">Password</Label>
				<Input
					id="password"
					value={password}
					onChange={(event) => updateState("password", event)}
					required
					disabled={isLoginLoading}
					type="password"
					onKeyDown={(event) => {
						if (event.key === "Enter" && isLoginReady) {
							// If the user hits Enter, have them attempt to log in
							passwordLogin();
						}
					}}
					ref={passwordInputRef}
				/>
				{showError && (
					<p className="text-sm">
						Username or password is incorrect.
					</p>
				)}
			</div>
			<Button
				onClick={passwordLogin}
				disabled={!isLoginReady || isLoginLoading}
			>
				{isLoginLoading ? "Logging in..." : "Log in"}
			</Button>
		</div>
	);
};
