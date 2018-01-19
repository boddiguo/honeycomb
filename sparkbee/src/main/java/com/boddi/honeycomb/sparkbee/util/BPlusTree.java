package com.boddi.honeycomb.sparkbee.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Vector;

class BPlusTree {

  private static Node tree;
  private static int degree;
  private static boolean debug;

  private BPlusTree(int x) {
    // a B+ Tree must have an initial degree
    degree = x;

    // The initial type of Node for a B+Tree is a leaf
    tree = new LeafNode(degree);

    debug = false;
  }

  private static void executeCommand(Command c, BufferedWriter output) throws InvalidCommandException, IOException {
    // execute command, does as it says, calls the appropriate procedure to accomplish the command
    // There are also some debug options to help the user see what's going on
    switch( (int) c.getCommand() ) {
      case 'd':
        if( c.getXValue() == 1 && !debug) {
          debug = true;
          System.out.println("ENTERING DEBUG MODE");
        }
        else if ( c.getXValue() == 0 && debug) {
          debug = false;
          System.out.println("EXITTING DEBUG MODE");
        }
        else if (c.getXValue() != 0 || c.getXValue() != 1){
          throw new InvalidCommandException("Invalid Operand with command d. Must be 0 or 1.");
        }
      case 'p':
        if(debug) {
          System.out.println("PRINTING TREE");
        }
        printTree(output);
        break;
      case 's':
        if(debug) {
          System.out.println("SEARCHING TREE FOR x = " + c.getXValue());
        }
        searchTree(c.getXValue(), output);
        break;
      case 'i':
        if(debug) {
          System.out.println("INSERTING x = " + c.getXValue() + " INTO THE TREE");
        }
        insertIntoTree(new DataNode(c.getXValue()));
        break;
    }
    if(debug && (int)c.getCommand() != 'p') {
      printTree(new BufferedWriter(new PrintWriter(System.out)));
      System.out.println("--->OPERATION COMPLETE");
    }
  }

  private static void insertIntoTree(DataNode dnode) {
    tree = tree.insert(dnode);
  }

  private static void searchTree(int x, BufferedWriter output) throws IOException {

    // search the tree starting from the top
    if( tree.search(new DataNode(x)) ) {
      output.write("FOUND" + System.getProperty("line.separator"));
    }
    else {
      output.write("NOT FOUND" + System.getProperty("line.separator"));
    }
  }

  @SuppressWarnings("unchecked")
  private static void printTree(BufferedWriter output) throws IOException {
    // create a vector to store all the nodes from each level as we
    Vector<Node> nodeList = new Vector();

    // put the root of the tree onto the stack to start the process
    nodeList.add(tree);

    boolean done = false;
    while(! done) {
      // this vector will hold all the children of the nodes in the current level
      Vector<Node> nextLevelList = new Vector();
      String toprint = "";

      // for each node in the list convert it to a string and add any children to the nextlevel stack
      for(int i=0; i < nodeList.size(); i++) {

        // get the node at position i
        Node node = (Node)nodeList.elementAt(i);

        // convert the node into a string
        toprint += node.toString() + " ";

        // if this is a leaf node we need only print the contents
        if(node.isLeafNode()) {
          done = true;
        }
        // if this is a tree node print the contents and populate
        // the temp vector with nodes that node i points to
        else
        {
          for(int j=0; j < node.size()+1 ; j++) {
            nextLevelList.add( ((TreeNode)node).getPointerAt(j) );
          }
        }
      }

      // print the level
      output.write(toprint + System.getProperty("line.separator"));

      // go to the next level and print it
      nodeList = nextLevelList;
    }
  }

  private static void readDegree(BufferedReader in) {
    // get the tree's degree from input
    try {
      int x = Integer.parseInt(in.readLine().trim());

      // set the degree of the tree
      new BPlusTree(x);

    } catch (Exception e1) {
      System.err.println("degree could not be read... defaulting to order 3");
      new BPlusTree(3);
    }
  }

  public static void main(String[] args) throws IOException {
    // if there are too many arguments error
    if(args.length > 1) {
      System.err.println("Syntax error in call sequence, use:\n\tjava BplusTree");
    }
    else {

      // declare a reader on Standard in, incase the file reader fails
      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

      // create a new file to store output
      BufferedWriter output = new BufferedWriter( new FileWriter(new File("bplustree.out")) );
//      try {
//        in = new BufferedReader(new InputStreamReader(BPlusTree.class.getClassLoader().getResourceAsStream("bplustree.inp")));
//      } catch (Exception e) {
//        e.printStackTrace();
//        System.err.println("Error: specified file not found (defaulting to standard input)");
//      }

      // get the degree of the B+Tree
      readDegree(in);

      // declare a new command object
      Command c = new Command();

      // continue executing commands until quit is reached
      while(c.getCommand() != 'q') {
        try {
          // read a command from input
          c.readCommand(in);

          // execute the command
          executeCommand(c, output);
        }
        catch (IOException e) {
          e.printStackTrace();
        }
        catch (InvalidCommandException e) {
          System.err.println(e.getMessage());
          System.out.println("Valid Query-Modes:\n\ti x - insert x into tree\n\ts x - find x in tree\n\tp   - print tree\n\tq   - quit");
        }
        catch (NumberFormatException e) {
          System.err.println("This type of command requires a integer operand");
          System.out.println("Valid Query-Modes:\n\ti x - insert x into tree\n\ts x - find x in tree\n\tp   - print tree\n\tq   - quit");
        }
        catch (Exception e) {
          e.printStackTrace();
          System.exit(-1);
        }
      }

      // close input and output
      output.close();
      in.close();

      // output.write("Exitting");
      System.exit(0);
    }
  }
}

class Command {
  int xvalue;
  char command;

  Command() {
    command = 0;
    xvalue = 0;
  }

  public String toString() {
    return "command = " + command + " x = " + xvalue;
  }

  public void readCommand(BufferedReader in) throws InvalidCommandException, IOException, NumberFormatException {

    boolean readcommand = false;

    // until a command has been read (valid or invalid) get something from input
    while(!readcommand && in.ready()) {
      command = (char)in.read();

      // if the line is a comment output the rest of the line
      if(command == '#') {
        // print the comment to the screen
        System.out.println( in.readLine() );
      }
      // if a valid command was read get any necessary arguments
      else if(this.validCommand()) {
        if(this.commandWithArgument()) {
          xvalue = Integer.parseInt(in.readLine().trim());
        }
        else {
          xvalue = 0;
          in.readLine();
        }
        readcommand = true;
      }
      else {
        // clean up the rest of the garbage on the input line
        in.readLine();
        throw new InvalidCommandException(command);
      }
    }
  }

  public char getCommand() {
    return command;
  }

  public int getXValue() {
    return xvalue;
  }

  private boolean validCommand() {
    return commandWithArgument() || commandWithoutArgument();
  }
  // list of commands that have an argument
  private boolean commandWithArgument() {
    return this.command == 'i' || this.command == 's' || this.command == 'd';
  }
  // list of commands that don't have arguments
  private boolean commandWithoutArgument() {
    return this.command == 'p' || this.command == 'q';
  }
}

// Class to handle occurances of invalid commands
class InvalidCommandException extends Exception {
  private static final long serialVersionUID = -2169157330841961180L;

  InvalidCommandException(char command) {
    super("Error: invalid query-mode \"" + command + "\" entered");
  }
  InvalidCommandException(String s) {
    super(s);
  }
}

// The node class is an abstract class
// its subclasses are LeafNode and TreeNode

