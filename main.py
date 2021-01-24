import fire
import subprocess

def init():
    bashCommand = "git init"
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    return

def stage():
    bashCommand = 'git add .'
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    return

def commit(message: str):
    bashCommand = 'git add .'
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    bashCommand = 'git commit -m'.split()
    bashCommand.append("%s" %(message))
    process = subprocess.Popen(bashCommand, stdout=subprocess.PIPE)
    return

def push():
    bashCommand = 'git push origin master'
    process = subprocess.Popen(bashCommand.split, stdout=subprocess.PIPE)

if __name__ == "__main__":
    fire.Fire({
        "init": init,
        "stage": stage,
        "commit": commit,
        "push": push
    })
