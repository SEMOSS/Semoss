#!/usr/bin/env python3
"""
propagation_supervisor_poc.py — prove the mechanism SEMOSS needs for
"user is granted a NEW project mid-session and wants to use it live":

  * A persistent interpreter runs in its own user+mount namespace, pivoted
    into a MINIMAL root so it cannot see the backing store or other users'
    projects.  It holds in-memory state (x = 1).
  * A TRUSTED supervisor (stands in for Java) keeps the backing store in its
    OWN mount view, invisible to the interpreter.
  * After the interpreter is already running, the supervisor injects projects
    into it via shared->slave mount propagation.  This includes a project that
    was NEVER staged at launch (the "granted mid-session" case).
  * The interpreter reads each injected project on its next command, cannot see
    un-injected projects or the backing root, and keeps x = 1 throughout.

Everything here is UNPRIVILEGED (no root, no CAP_SYS_ADMIN in the host ns); it
only needs unprivileged user namespaces enabled (flip the container seccomp
profile — see sandbox_probe.sh).  Run: python3 scripts/propagation_supervisor_poc.py
"""
import ctypes, os, sys, tempfile, shutil

libc = ctypes.CDLL("libc.so.6", use_errno=True)
CLONE_NEWNS=0x00020000; CLONE_NEWUSER=0x10000000
MS_BIND=0x1000; MS_REC=0x4000; MS_RDONLY=1
MS_PRIVATE=1<<18; MS_SLAVE=1<<19; MS_SHARED=1<<20
MNT_DETACH=2; SYS_pivot_root=155

def chk(r, what):
    if r != 0:
        e = ctypes.get_errno(); raise OSError(e, "%s: %s" % (what, os.strerror(e)))
def mount(src,tgt,fs,flags,data=None):
    return libc.mount(src.encode() if src else None, tgt.encode(),
                      fs.encode() if fs else None, ctypes.c_ulong(flags),
                      data.encode() if data else None)
def pivot_root(new,old): return libc.syscall(SYS_pivot_root, new.encode(), old.encode())

# ---- prerequisite: unprivileged userns -----------------------------------
uid,gid=os.getuid(),os.getgid()
try:
    if libc.unshare(CLONE_NEWUSER)!=0:
        raise OSError(ctypes.get_errno(),"unshare(CLONE_NEWUSER)")
except OSError as e:
    print("PREREQ NOT MET: cannot create user namespace unprivileged (%s)." % e)
    print("Flip the container seccomp profile first (see sandbox_probe.sh §1). Exiting 0.")
    sys.exit(0)
open("/proc/self/setgroups","w").write("deny")
open("/proc/self/gid_map","w").write("0 %d 1"%gid)
open("/proc/self/uid_map","w").write("0 %d 1"%uid)
chk(libc.unshare(CLONE_NEWNS),"unshare mountns(supervisor)")
chk(mount("none","/","",MS_REC|MS_PRIVATE),"make-rprivate /")

# ---- lay out backing store (only the supervisor will see this) -----------
WORK=tempfile.mkdtemp()
BACKING=os.path.join(WORK,"backing")
for proj,content in [("projectB","B_DATA"),("projectC","C_DATA"),("otherUser_secret","SHOULD_NOT_LEAK")]:
    os.makedirs(os.path.join(BACKING,proj))
    open(os.path.join(BACKING,proj,"data.csv"),"w").write(content)

# ---- build the interpreter's minimal root with a shared 'projects' portal -
JAIL=os.path.join(WORK,"jail"); PORTAL=os.path.join(JAIL,"projects"); OLD=os.path.join(JAIL,"oldroot")
os.makedirs(PORTAL); os.makedirs(OLD)
chk(mount(JAIL,JAIL,"",MS_BIND),"bind jail->self")          # new root must be a mount
chk(mount(PORTAL,PORTAL,"",MS_BIND),"bind portal->self")
chk(mount("none",PORTAL,"",MS_SHARED),"make-shared portal") # supervisor side = shared

c2p_r,c2p_w=os.pipe(); p2c_r,p2c_w=os.pipe()                # child<->supervisor pipes
pid=os.fork()

# ====================== INTERPRETER (child) ===============================
if pid==0:
    os.close(c2p_r); os.close(p2c_w)
    chk(libc.unshare(CLONE_NEWNS),"child unshare ns")
    chk(mount("none",PORTAL,"",MS_SLAVE),"child make-slave portal")  # child side = slave (receives)
    chk(pivot_root(JAIL,OLD),"pivot_root")
    os.chdir("/")
    chk(libc.umount2(b"/oldroot",MNT_DETACH),"detach oldroot")  # backing now invisible to child
    x=1
    def emit(s): os.write(c2p_w,(s+"\n").encode())
    emit("READY x=%d"%x)
    buf=b""
    while True:
        ch=os.read(p2c_r,1)
        if not ch: break
        if ch!=b"\n": buf+=ch; continue
        line=buf.decode(); buf=b""
        if line=="EXIT": emit("BYE"); os._exit(0)
        elif line=="STATE": emit("STATE x=%d"%x)
        elif line=="LIST":
            try: emit("LIST %s"%",".join(sorted(os.listdir("/projects"))))
            except Exception as ex: emit("LIST_ERR %s"%type(ex).__name__)
        elif line.startswith("READ "):
            p=line[5:]
            try: emit("READ_OK %s=%s"%(p,open(p).read().strip()))
            except Exception as ex: emit("READ_FAIL %s (%s)"%(p,type(ex).__name__))
        elif line=="PEEK_BACKING":
            emit("BACKING_VISIBLE %s"%os.path.exists("/oldroot") or os.path.exists("%s"%BACKING))
        else: emit("UNKNOWN %s"%line)
    os._exit(0)

# ====================== SUPERVISOR (parent, = "Java") =====================
os.close(c2p_w); os.close(p2c_r)
def send(cmd): os.write(p2c_w,(cmd+"\n").encode())
_acc=b""
def recv():
    global _acc
    while b"\n" not in _acc:
        chunk=os.read(c2p_r,4096)
        if not chunk: break
        _acc+=chunk
    line,_,_acc=_acc.partition(b"\n")
    return line.decode()
def inject(proj):  # supervisor binds backing/proj into the shared portal -> propagates into child
    tgt=os.path.join(PORTAL,proj); os.makedirs(tgt,exist_ok=True)
    chk(mount(os.path.join(BACKING,proj),tgt,"",MS_BIND|MS_REC),"inject %s"%proj)

P=lambda ok,msg: print(("  [PASS] " if ok else "  [FAIL] ")+msg)
print("=== propagation supervisor PoC (unprivileged) ===")
print("  "+recv())                                              # READY x=1
send("STATE");        P(recv()=="STATE x=1","interpreter holds x=1")
send("READ /projects/projectB/data.csv")
P(recv().startswith("READ_FAIL"),"projectB not visible before injection")

print("--- supervisor injects projectB (lazy load of an authorized project) ---")
inject("projectB")
send("READ /projects/projectB/data.csv")
P(recv()=="READ_OK /projects/projectB/data.csv=B_DATA","interpreter reads projectB live")

print("--- BRAND-NEW mid-session grant: inject projectC (never staged at launch) ---")
inject("projectC")
send("READ /projects/projectC/data.csv")
P(recv()=="READ_OK /projects/projectC/data.csv=C_DATA","interpreter reads NEWLY-granted projectC live")

print("--- isolation: interpreter must NOT see un-injected data or the backing root ---")
send("READ /projects/otherUser_secret/data.csv")
P(recv().startswith("READ_FAIL"),"cannot read a project that was never injected")
send("READ %s/otherUser_secret/data.csv"%BACKING)
P(recv().startswith("READ_FAIL"),"cannot reach the backing store by absolute path")
send("LIST"); print("  "+recv()+"  (only injected projects present)")

print("--- state survived every injection ---")
send("STATE"); P(recv()=="STATE x=1","x is STILL 1 — interpreter never restarted")
send("EXIT"); recv()
os.waitpid(pid,0); shutil.rmtree(WORK,ignore_errors=True)
print("=== done ===")
