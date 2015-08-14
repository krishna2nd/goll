package scp

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"golang.org/x/crypto/ssh"
)

type ScpInfo struct {
	User     string
	Identity string
	Server   string

	clientConfig *ssh.ClientConfig
	client       *ssh.Client
	//session      *ssh.Session
}

type ScpController struct {
	Connection chan int
	Session    chan int
}

var ParallelController = make(map[string]*ScpController)

func AddScpController(server string, connection_limit, session_limit int) {
	ParallelController[server] = &ScpController{
		Connection: make(chan int, session_limit),
		Session:    make(chan int, connection_limit),
	}
}

const token = 1

type Scp struct {
	ScpInfo
}

func (this *Scp) Copy(size int64, mode os.FileMode, fileName string, contents io.Reader, destinationPath string) error {
	return this.copy(size, mode, fileName, contents, destinationPath)
}

func (this *Scp) File(filePath, destinationPath string) error {
	file, err := os.Open(filePath)
	if nil != err {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if nil != err {
		return err
	}

	return this.copy(stat.Size(), stat.Mode().Perm(), path.Base(filePath), file, destinationPath)
}

func (this *Scp) copy(size int64, mode os.FileMode, fileName string, contents io.Reader, destination string) error {
	var (
		session *ssh.Session
		err     error
	)
	
    ParallelController[this.Server].Session <- token
	session, err = this.client.NewSession()
	if nil != err {
		return errors.New("Failed to create new session" + err.Error())
	}

	go func() {
		w, _ := session.StdinPipe()
		defer w.Close()

		fmt.Fprintf(w, "C%#o %d %s\n", mode, size, fileName)
		io.Copy(w, contents)
		fmt.Fprint(w, "\x00")
		<-ParallelController[this.Server].Session
	}()
	cmd := fmt.Sprintf("scp -t %s", destination)
	if err := session.Run(cmd); nil != err {
		return err
	}
	session.Close()
	return nil
}

func NewScp(target_server, user, identity string) (*Scp, error) {
	return new(Scp).Init(target_server, user, identity)
}

func (this *Scp) Init(target_server, user, identity string) (*Scp, error) {

	this.Server = target_server
	this.User = user
	this.Identity = identity

	AuthKey, err := PublicKeyFile(this.Identity)

	if nil != err {
		return nil, err
	}

	this.clientConfig = &ssh.ClientConfig{
		User: this.User,
		Auth: []ssh.AuthMethod{AuthKey},
	}

	ParallelController[this.Server].Connection <- token
	this.client, err = ssh.Dial("tcp", this.Server+":22", this.clientConfig)

	if nil != err {
		return nil, errors.New("Failed to dial: " + err.Error())
	}

	return this, nil
}

func (this *Scp) Close() {
	<-ParallelController[this.Server].Connection
}

func PublicKeyFile(file string) (ssh.AuthMethod, error) {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.New("PublicKeyFile: " + err.Error())
	}
	key, err := ssh.ParsePrivateKey(buffer)
	if err != nil {
		return nil, errors.New("PublicKeyFile: " + err.Error())
	}

	return ssh.PublicKeys(key), nil
}
