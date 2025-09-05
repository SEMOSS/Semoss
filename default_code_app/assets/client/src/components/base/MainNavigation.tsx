import {
    Button,
    IconButton,
    Stack,
    styled,
    Typography,
    useTheme,
} from '@mui/material';
import { SemossBlueLogo } from '@/assets';
import { useNavigate } from 'react-router';
import { ROUTE_PATH_ANIMAL_PAGE } from '@/pages';
import { AccountCircle } from '@mui/icons-material';
import { useInsight } from '@semoss/sdk-react';
import { useState } from 'react';
import { UserProfileMenu } from './UserProfileMenu';

// A Stack with a different-colored background
const StyledStack = styled(Stack)(({ theme }) => ({
    backgroundColor: theme.palette.background.paper,
    height: theme.spacing(8),
}));

// A Stack that changes the cursor to a finger to show the user that they can click
const CursorStack = styled(Stack)({
    cursor: 'pointer',
});

// The list of the buttons that should be displayed
const navigationButtons: {
    path: string;
    text: string;
}[] = [
    {
        path: '/',
        text: 'Home',
    },
    {
        path: ROUTE_PATH_ANIMAL_PAGE,
        text: 'Animal Page',
    },
];

/**
 * The main navigation bar allowing users to move between pages, if they are authorized.
 *
 * @component
 */
export const MainNavigation = () => {
    const { isAuthorized } = useInsight(); // Read whether the user is authorized, so that buttons only work if they are
    const { spacing } = useTheme();
    const navigate = useNavigate();

    /**
     * State
     */
    const [userMenuAnchorEl, setUserMenuAnchorEl] = useState<null | Element>(
        null,
    );

    return (
        <StyledStack
            direction="row"
            alignItems="center"
            justifyContent="space-between"
            spacing={2}
            paddingX={2}
        >
            <Stack direction="row" alignItems="center" spacing={2}>
                {/* Display the logo and the title, and have clicking them take users home */}
                <CursorStack
                    direction="row"
                    alignItems="center"
                    spacing={2}
                    // Only let them navigate when authorized
                    onClick={isAuthorized ? () => navigate('/') : undefined}
                >
                    <img
                        src={SemossBlueLogo}
                        alt="Semoss Blue Logo"
                        height={spacing(6)}
                    />
                    <Typography
                        variant="h4"
                        fontWeight="bold"
                        whiteSpace="nowrap"
                    >
                        SEMOSS Blank Canvas
                    </Typography>
                </CursorStack>

                {/* Display the navigation buttons when authorized */}
                {isAuthorized &&
                    navigationButtons.map((page) => (
                        <Button
                            key={page.path}
                            onClick={() => navigate(page.path)}
                            color="inherit"
                        >
                            {page.text}
                        </Button>
                    ))}
            </Stack>

            {/* If the user is logged in, allow them to see their info */}
            {isAuthorized && (
                <>
                    <IconButton
                        title="View user menu"
                        onClick={(e) => setUserMenuAnchorEl(e.currentTarget)}
                    >
                        <AccountCircle />
                    </IconButton>

                    <UserProfileMenu
                        open={Boolean(userMenuAnchorEl)}
                        anchorEl={userMenuAnchorEl}
                    />
                </>
            )}
        </StyledStack>
    );
};
