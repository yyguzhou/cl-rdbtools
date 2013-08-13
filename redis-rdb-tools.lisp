(eval-when (:compile-toplevel :load-toplevel :execute)
  (use-package :sb-ext))

(defclass hook-func () ())

(defgeneric parse-magic-hook (hook-func result))
(defgeneric parse-rdb-version-hook (hook-func result))
(defgeneric parse-string-hook (hook-func result))
(defgeneric parse-integer-hook (hook-func result))
(defgeneric parse-ziplist-entry-hook (hook-func result))
(defgeneric parse-key-hook (hook-func result))
(defgeneric parse-db-number-hook (hook-func result))

(defclass default-hook (hook-func) ())

(defmethod parse-magic-hook ((hook default-hook) result)
  (format t "[MAGIC]~c~c~a~%" (code-char 9) (code-char 9) result))

(defmethod parse-rdb-version-hook ((hook default-hook) result)
  (format t "[VERSION]~c~a~%" (code-char 9) result))

(defmethod parse-string-hook ((hook default-hook) result)
  (format t "[STRING]~c~a~%" (code-char 9) result))

(defmethod parse-integer-hook ((hook default-hook) result)
  (format t "[INTEGER]~c~a~%" (code-char 9) result))

(defmethod parse-ziplist-entry-hook ((hook default-hook) result)
  (format t "[ENTRY]~c~c~a~%" (code-char 9) (code-char 9) result))

(defmethod parse-key-hook ((hook default-hook) result)
  (format t "[KEY]~c~c~a~%" (code-char 9) (code-char 9) result))

(defmethod parse-db-number-hook ((hook default-hook) result)
  (format t "[DB]~c~c~a~%" (code-char 9) (code-char 9) result))

(defvar *hook* (make-instance 'default-hook))

(let* ((dbs (make-list 16))
       (cur-db nil))
  (dotimes (x (list-length dbs))
    (setf (nth x dbs) (make-hash-table :test #'equal)))
  (setf cur-db (nth 0 dbs))
  (defun select-db (n)
    (setf cur-db (nth n dbs)))
  (defun db-set (k v)
    (setf (gethash k cur-db) v))
  (defun db-get (k)
    (gethash k cur-db)))

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
;    (format t "type ~a len ~a flag ~a~%" type len flag)
    (case type
      ((#x00 #x40 #x80) (values (parse-string in len) 'rdb-string))
      (#xC0 (case flag
              ((0 1 2) (values (parse-int in flag) 'rdb-integer))
              (t (values (parse-compressed-string in) 'rdb-compressed-string)))))))

(defun parse-sequence (in n)
  (dotimes (x n)
    (multiple-value-bind (result type) (parse-redis-obj in)
      (case type
        (rdb-integer (parse-integer-hook *hook* result))
        (rdb-string (parse-string-hook *hook* result))
        (rdb-compressed-string (parse-string-hook *hook* result))))))

(defun parse-ziplist-entry (in)
  (let* ((next (read-byte in))
         (prev-len (cond ((> next 253) (read-dword-le in))
                         (t next)))
         (flag (read-byte in))
         (len (case (logand #xC0 flag)
                (0 (logand #x3F flag))
                (#x40 (logior (ash (logand #x3F flag) 8) (read-byte in)))
                (#x80 (read-dword in))
                (t nil))))
    (if len
        (read-string in len)
        (case (logand flag #x30)
          (0 (read-word-le in))
          (#x10 (read-dword-le in))
          (#x20 (read-qword-le in))
          (t (case (logand flag #x0F)
               (0 (logior (read-byte in) (ash (read-word-le in) 8)))
               (#x0E (read-byte in))
               (t (logand flag #x0F))))))))

(defun parse-ziplist (in)
  (parse-length in)
  (let* ((zlbytes (read-dword-le in))
         (zltail (read-dword-le in))
         (zllen (read-word-le in)))
;    (format t "zlbytes ~a zltail ~a zllen ~a~%" zlbytes zltail zllen)
    (dotimes (i zllen)
      (parse-ziplist-entry-hook *hook* (parse-ziplist-entry in)))
    (read-byte in)))

(defun parse-hash (in)
  (multiple-value-bind (type len) (parse-length in)
    (declare (ignore type))
    (parse-sequence in (* 2 len))))

(defun parse-list-set (in)
  (multiple-value-bind (type len) (parse-length in)
    (declare (ignore type))
    (parse-sequence in len)))

(defun parse-simple (in)
  (multiple-value-bind (val type) (parse-redis-obj in)
    (case type
      (rdb-integer (parse-integer-hook *hook* val))
      (rdb-string (parse-string-hook *hook* val))
      (rdb-compressed-string (parse-string-hook *hook* val)))))

(defun parse-db (in)
  (parse-db-number-hook *hook* (read-byte in))
  (loop for next = (read-byte in) then (read-byte in)
     until (not (find next '(0 1 2 3 4 9 10 11 12 13)))
     do
       (parse-key-hook *hook* (parse-redis-obj in))
       (case next
         ((10 13) (parse-ziplist in))
         (#xFD (format t "expiry time in seconds~%") next)
         (#xFC (format t "expiry time in ms~%") next)
         (0 (parse-simple in))
         ((1 2 3) (parse-list-set in))
         (4 (parse-hash in)))))

(defun parse-rdb (in)
  (parse-magic-hook *hook* (read-string in 5))
  (parse-rdb-version-hook *hook* (read-string in 4))
  (loop for next = (read-byte in) then (parse-db in)
      until (not (eql #xFE next))))

(defun main ()
  (if (not (eql 2 (length *posix-argv*)))
      (progn (format t "Useage: ~a redis-rdb-file~%" (first *posix-argv*))
             (exit))
      (with-open-file (s (nth 1 *posix-argv*)
                         :direction :input
                         :element-type '(unsigned-byte 8)
                         :if-does-not-exist :error)
        (parse-rdb s))))

(main)