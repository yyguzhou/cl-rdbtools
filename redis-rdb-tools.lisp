(eval-when (:compile-toplevel :load-toplevel :execute)
  (use-package :sb-ext))

(defun read-string (in n)
  (let ((arr (make-array n)))
    (read-sequence arr in :end n)
    (map 'string #'code-char arr)))

(defun read-word (in)
  (logior (ash (read-byte in nil 0) 8) (read-byte in nil 0)))

(defun read-dword (in)
  (logior (ash (read-word in) 16) (read-word in)))

(defun read-qword (in)
  (logior (ash (read-dword in) 32) (read-dword in)))

(defun read-word-le (in)
  (logior (read-byte in nil 0) (ash (read-byte in nil 0) 8)))

(defun read-dword-le (in)
  (logior (read-word-le in) (ash (read-word-le in) 16)))

(defun read-qword-le (in)
  (logior (read-dword-le in) (ash (read-dword-le in) 32)))

(defun parse-string (in len)
  (read-string in len))

(defun parse-int (in flag)
  (case flag
    (0 (read-byte in))
    (1 (read-word-le in))
    (2 (read-dword-le in))))

(defun parse-compressed-string (in)
  (multiple-value-bind (ctype clen) (parse-length in)
    (declare (ignore ctype))
    (multiple-value-bind (type len) (parse-length in)
      (declare (ignore type len))
      (parse-string in clen))))

(defun parse-length (in)
  (let* ((next (read-byte in))
         (type (logand next #xC0))
         (flag (logand next #x3F))
         (len nil))
    (case type
      (#x00 (setf len flag))
      (#x40 (setf len (logior (ash flag 8) (read-byte in))))
      (#x80 (setf len (read-dword in)))
      (#xC0 (case flag
              (0 (setf len 1))
              (1 (setf len 2))
              (2 (setf len 4)))))
    (values type len flag)))

(defun parse-redis-obj (in)
  (multiple-value-bind (type len flag) (parse-length in)
    (case type
      ((#x00 #x40 #x80) (values (parse-string in len) 'rdb-string))
      (#xC0 (case flag
              ((0 1 2) (values (parse-int in flag) 'rdb-integer))
              (t (values (parse-compressed-string in) 'rdb-compressed-string)))))))

(defun parse-sequence (in n)
  (dotimes (x n)
    (multiple-value-bind (result type) (parse-redis-obj in)
      (case type
        ('rdb-integer (format t "integer: "))
        ('rdb-string (format t "string: "))
        ('rdb-compressed-string (format t "compressed string: ")))
      (format t "~a~%" result))))

(defun parse-hash (in)
  (multiple-value-bind (type len) (parse-length in)
    (declare (ignore type))
    (parse-sequence in (* 2 len))))

(defun parse-list-set (in)
  (multiple-value-bind (type len) (parse-length in)
    (declare (ignore type))
    (parse-sequence in len)))

(defun parse-db (in)
  (format t "DB Number: ~a~%" (read-byte in))
;  (let ((next (read-byte in)))
;    (format t "next ~a~%" next)))
  (loop for next = (read-byte in) then (read-byte in)
     until (not (find next '(0 1 2 3 4 13)))
     do
       (format t "Key: ~a~%" (parse-redis-obj in))
       (case next
         (13 (format t "Ziplist: ~a~%" (parse-redis-obj in)))
         (#xFD (format t "expiry time in seconds~%") next)
         (#xFC (format t "expiry time in ms~%") next)
         (0 (format t "String: ~a~%" (parse-redis-obj in)))
         ((1 2 3) (format t "List or Set: ~a~%" (parse-list-set in)))
         (4 (format t "Hash: ~a~%" (parse-hash in))))))

(defun parse-rdb (in)
  (let* ((magic-str (read-string in 5))
         (rdb-ver (read-string in 4)))
    (format t "magic string: ~a~%RDB Version: ~a~%"
            magic-str rdb-ver)
    (do ((next (read-byte in) (parse-db in)))
        ((not (eql #xFE next)))
      (format t "next is ~a~%" next))))

(defun main ()
  (if (not (eql 2 (length *posix-argv*)))
      (progn (format t "Useage: ~a redis-rdb-file~%" (first *posix-argv*))
             (exit))
      (with-open-file (s (nth 1 *posix-argv*)
                         :direction :input
                         :element-type '(unsigned-byte 8)
                         :if-does-not-exist :error)
        (format t "parse-db ~a~%" (parse-rdb s)))))

(main)