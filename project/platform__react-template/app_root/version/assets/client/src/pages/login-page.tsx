import { useInsight } from "@semoss/sdk/react";
import { type FormEvent, useState } from "react";
import { Navigate, useLocation, useNavigate } from "react-router";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

/**
 * Native username/password login. Signed-in users are sent on to where they were headed.
 */
export const LoginPage = () => {
	const { isAuthorized, actions } = useInsight();
	const { state } = useLocation();
	const navigate = useNavigate();

	const [username, setUsername] = useState("");
	const [password, setPassword] = useState("");
	const [isSubmitting, setIsSubmitting] = useState(false);
	const [errorMessage, setErrorMessage] = useState<string | null>(null);

	// AuthorizedLayout passes the page the user was trying to reach
	const target = (state as { target?: unknown } | null)?.target;
	const redirectTo = typeof target === "string" ? target : "/";

	if (isAuthorized) {
		return <Navigate to={redirectTo} replace />;
	}

	const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
		event.preventDefault();
		setIsSubmitting(true);
		setErrorMessage(null);

		try {
			const success = await actions.login({
				type: "native",
				username,
				password,
			});
			if (success) {
				navigate(redirectTo, { replace: true });
				return;
			}
			setErrorMessage("Username or password is incorrect.");
		} catch (error) {
			setErrorMessage(
				error instanceof Error ? error.message : "Unable to log in.",
			);
		} finally {
			setIsSubmitting(false);
		}
	};

	return (
		<div className="flex h-full items-center justify-center p-6">
			<Card className="w-full max-w-sm">
				<CardHeader>
					<CardTitle>Log in</CardTitle>
					<CardDescription>Use your SEMOSS account.</CardDescription>
				</CardHeader>
				<CardContent>
					<form className="flex flex-col gap-4" onSubmit={onSubmit}>
						<div className="flex flex-col gap-2">
							<Label htmlFor="username">Username</Label>
							<Input
								id="username"
								autoComplete="username"
								value={username}
								onChange={(event) => {
									setErrorMessage(null);
									setUsername(event.target.value);
								}}
								disabled={isSubmitting}
								required
							/>
						</div>
						<div className="flex flex-col gap-2">
							<Label htmlFor="password">Password</Label>
							<Input
								id="password"
								type="password"
								autoComplete="current-password"
								value={password}
								onChange={(event) => {
									setErrorMessage(null);
									setPassword(event.target.value);
								}}
								disabled={isSubmitting}
								required
							/>
						</div>
						{errorMessage && (
							<p className="text-sm text-destructive" role="alert">
								{errorMessage}
							</p>
						)}
						<Button
							type="submit"
							disabled={!username || !password || isSubmitting}
						>
							{isSubmitting ? "Logging in..." : "Log in"}
						</Button>
					</form>
				</CardContent>
			</Card>
		</div>
	);
};
