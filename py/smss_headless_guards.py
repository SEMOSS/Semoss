"""
Neutralise the other ways user code can pop a window or block on a human.

smss_inline_display handles matplotlib, which is the one library currently in
pyproject.toml that can do this. Everything guarded here is defensive: none of
these packages ship with SEMOSS today, so every guard is dormant until the
package actually turns up in sys.modules. The point is that adding opencv or
plotly to pyproject.toml later should not resurrect the class of bug that
matplotlib had.

The failure modes are all the same shape as the matplotlib one. User code runs
on a socketserver worker thread inside a long lived process with no display and
no usable stdin, so:

  * cv2.imshow needs a GUI window, and cv2.waitKey blocks until a keypress that
    can never arrive.
  * plotly's fig.show() falls back to the "browser" renderer and shells out to
    a browser that does not exist.
  * PIL's Image.show() spawns the OS image viewer.
  * tkinter from a non main thread aborts the process on macOS - this is the
    exact mechanism behind the original matplotlib crash.
  * input() and getpass() block forever on a stdin nobody writes to. Java never
    feeds the child's stdin, so the execution simply never returns.

Where there is something sensible to show, the guard renders it inline through
smss_inline_display so the call does what the user meant - cv2.imshow and
Image.show produce an image in the console, the way they would in a notebook.
Where there is not, the guard explains itself on the console rather than
hanging or dying.
"""

import io
import sys
import threading

import smss_inline_display as display

# Root package names worth re-checking on import. Guards are keyed by root so
# the import hook can dispatch with a single set lookup.
_GUARD_ROOTS = frozenset(
    [
        "cv2",
        "plotly",
        "PIL",
        "webbrowser",
        "tkinter",
        "getpass",
    ]
)

# Roots still waiting for their package to show up. Guards remove themselves as
# they install, so once this is empty the import hook stops calling in.
_pending = set(_GUARD_ROOTS)

_install_lock = threading.Lock()
# Same re-entrancy protection as smss_inline_display: this runs inside
# builtins.__import__, and patching can itself trigger imports.
_installing = False


def guard_roots():
    """The root package names this module wants to hear about."""
    return _GUARD_ROOTS


def pending_roots():
    """Roots whose guard has not been installed yet."""
    return _pending


def maybe_install(root=None):
    """
    Install any guard whose package is now importable.

    Args:
        root (`str`): the root package just imported, or None to check them all.
            Passing it skips the other lookups.
    """
    global _installing
    if _installing or not _pending:
        return
    if root is not None and root not in _pending:
        return

    with _install_lock:
        if _installing:
            return
        _installing = True
        try:
            for name in list(_pending):
                if root is not None and name != root:
                    continue
                module = sys.modules.get(name)
                if module is None:
                    continue
                try:
                    if _INSTALLERS[name](module):
                        _pending.discard(name)
                except Exception:
                    # A guard that cannot install must never break the import
                    # that triggered it. Leaving it pending means we retry.
                    pass
        finally:
            _installing = False


def _explain(call, reason, suggestion=None):
    message = "[headless guard] " + call + " does nothing here: " + reason
    if suggestion:
        message += " " + suggestion
    display.emit_text(message)


# ---------------------------------------------------------------------------
# opencv
# ---------------------------------------------------------------------------


def _guard_cv2(cv2):
    """
    Render cv2.imshow inline and stop cv2.waitKey blocking forever.

    imencode gives back exactly the bytes a .png file would hold, and it takes
    the BGR ordering opencv already uses, so no colour conversion is needed.
    """
    if not hasattr(cv2, "imshow"):
        return False

    def _inline_imshow(winname, mat, *args, **kwargs):
        if not display.is_armed():
            return None
        try:
            ok, encoded = cv2.imencode(".png", mat)
        except Exception as e:
            _explain("cv2.imshow", "the image could not be encoded (" + str(e) + ").")
            return None
        if not ok:
            _explain("cv2.imshow", "the image could not be encoded.")
            return None
        display.emit_image_bytes(bytes(bytearray(encoded)))
        return None

    cv2.imshow = _inline_imshow

    # Returning "no key pressed" immediately is the only non hanging answer.
    def _no_wait(delay=0, *args, **kwargs):
        return -1

    for name in ("waitKey", "waitKeyEx", "pollKey"):
        if hasattr(cv2, name):
            setattr(cv2, name, _no_wait)

    # Window management has nothing to manage.
    def _noop(*args, **kwargs):
        return None

    for name in (
        "namedWindow",
        "destroyWindow",
        "destroyAllWindows",
        "moveWindow",
        "resizeWindow",
        "setWindowTitle",
        "setWindowProperty",
        "startWindowThread",
    ):
        if hasattr(cv2, name):
            setattr(cv2, name, _noop)

    return True


# ---------------------------------------------------------------------------
# plotly
# ---------------------------------------------------------------------------


def _guard_plotly(plotly):
    """
    Stop fig.show() reaching for a browser.

    Static export needs kaleido. When it is available the figure renders inline
    like any other; when it is not, say so rather than silently doing nothing -
    there is no way to turn a plotly figure into a PNG without it.
    """
    pio = sys.modules.get("plotly.io")
    basedatatypes = sys.modules.get("plotly.basedatatypes")
    if pio is None and basedatatypes is None:
        # plotly's root package is in sys.modules but its submodules are not
        # loaded yet. Retry on the next import.
        return False

    def _inline_show(fig, call="fig.show()"):
        if not display.is_armed():
            return None
        try:
            raw = fig.to_image(format="png")
        except Exception as e:
            _explain(
                call,
                "this python process has no browser, and rendering the figure "
                "to a static image failed (" + str(e) + ").",
                "Install kaleido for inline plotly images, or use "
                "fig.write_html(...) to save it.",
            )
            return None
        display.emit_image_bytes(raw)
        return None

    installed = False

    if basedatatypes is not None and hasattr(basedatatypes, "BaseFigure"):

        def _inline_method_show(self, *args, **kwargs):
            return _inline_show(self)

        basedatatypes.BaseFigure.show = _inline_method_show
        installed = True

    # pio.show(fig) is the free function form.
    if pio is not None and hasattr(pio, "show"):

        def _inline_pio_show(fig, *args, **kwargs):
            return _inline_show(fig, "plotly.io.show")

        pio.show = _inline_pio_show
        installed = True

    return installed


# ---------------------------------------------------------------------------
# pillow
# ---------------------------------------------------------------------------


def _guard_pil(pil):
    """Render Image.show() inline instead of spawning the OS image viewer."""
    image_module = sys.modules.get("PIL.Image")
    if image_module is None or not hasattr(image_module, "Image"):
        return False

    def _inline_image_show(self, title=None, **kwargs):
        if not display.is_armed():
            return None
        buffer = io.BytesIO()
        try:
            # PNG cannot hold every mode PIL supports, so fall back to a
            # convert for the ones it cannot (CMYK, F, I;16 and friends).
            source = self
            if source.mode not in ("1", "L", "LA", "P", "RGB", "RGBA"):
                source = source.convert("RGB")
            source.save(buffer, format="PNG")
        except Exception as e:
            _explain("Image.show()", "the image could not be encoded (" + str(e) + ").")
            return None
        display.emit_image_bytes(buffer.getvalue())
        return None

    image_module.Image.show = _inline_image_show
    return True


# ---------------------------------------------------------------------------
# webbrowser
# ---------------------------------------------------------------------------


def _guard_webbrowser(webbrowser):
    """There is no browser to open, and on a sandboxed worker no way to spawn one."""
    if not hasattr(webbrowser, "open"):
        return False

    def _blocked_open(url, *args, **kwargs):
        _explain(
            "webbrowser.open",
            "this python process has no browser.",
            "The url was: " + str(url),
        )
        return False

    for name in ("open", "open_new", "open_new_tab"):
        if hasattr(webbrowser, name):
            setattr(webbrowser, name, _blocked_open)

    return True


# ---------------------------------------------------------------------------
# tkinter
# ---------------------------------------------------------------------------


def _guard_tkinter(tkinter):
    """
    Refuse to build a Tk root.

    Tk off the main thread aborts the process on macOS. A raised exception is a
    traceback the user can read; the alternative is the whole worker vanishing
    and every other insight in it going with it.
    """
    if not hasattr(tkinter, "Tk"):
        return False

    def _blocked_tk_init(self, *args, **kwargs):
        raise RuntimeError(
            "tkinter is not usable here: this python process has no display, "
            "and building a Tk window off the main thread terminates the "
            "interpreter. Plot with matplotlib instead - figures are rendered "
            "inline automatically."
        )

    tkinter.Tk.__init__ = _blocked_tk_init
    return True


# ---------------------------------------------------------------------------
# stdin
# ---------------------------------------------------------------------------

_STDIN_MESSAGE = (
    "{0}() cannot be used here: this python process runs with no interactive "
    "stdin, so it would wait forever. Pass values in through the insight "
    "instead, for example with smss_get_runtime_var()."
)


def _guard_getpass(getpass):
    if not hasattr(getpass, "getpass"):
        return False

    def _blocked_getpass(prompt="Password: ", stream=None):
        raise RuntimeError(_STDIN_MESSAGE.format("getpass.getpass"))

    getpass.getpass = _blocked_getpass
    return True


def guard_stdin_builtins(builtins_module):
    """
    Replace input() with something that fails fast.

    Called at import time rather than lazily, because insight globals take a
    copy of the builtins dict when they are created and would otherwise keep
    the real input() forever.
    """
    if not hasattr(builtins_module, "input"):
        return

    def _blocked_input(prompt=""):
        raise RuntimeError(_STDIN_MESSAGE.format("input"))

    builtins_module.input = _blocked_input


_INSTALLERS = {
    "cv2": _guard_cv2,
    "plotly": _guard_plotly,
    "PIL": _guard_pil,
    "webbrowser": _guard_webbrowser,
    "tkinter": _guard_tkinter,
    "getpass": _guard_getpass,
}
