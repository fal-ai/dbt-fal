import fire
import subprocess

def init():
    bashCommand = "git init"
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    return

def commit(message: str):
    addCommand = 'git add .'
    process = subprocess.Popen(addCommand.split(), stdout=subprocess.PIPE)
    commitCommand = 'git commit -m'.split()
    commitCommand.append("%s" %(message))
    process = subprocess.Popen(commitCommand, stdout=subprocess.PIPE)
    return

def push():
    bashCommand = 'git push origin master'
    process = subprocess.Popen(bashCommand.split, stdout=subprocess.PIPE)

if __name__ == "__main__":
    fire.Fire({
        "init": init,
        "commit": commit,
        "push": push
    })
